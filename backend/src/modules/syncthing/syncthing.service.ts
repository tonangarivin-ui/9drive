import type { ConnectedAccount, File } from '@prisma/client'
import type { Response } from 'express'
import type { Readable } from 'node:stream'
import { prisma } from '../../config/prisma.js'
import { decryptText } from '../../utils/crypto.js'
import { createReadStream, createWriteStream } from 'node:fs'
import { mkdir, stat, unlink, readdir } from 'node:fs/promises'
import { join, dirname } from 'node:path'
import { pipeline } from 'node:stream/promises'

type FileWithAccount = File & { connectedAccount: ConnectedAccount }
type StreamOptions = { disposition?: 'inline' | 'attachment' }

interface SyncthingConfig {
  apiUrl: string
  apiKey: string
  folderId: string
  folderPath: string
  quotaBytes: bigint | null
}

function contentDisposition(type: 'inline' | 'attachment', fileName: string) {
  return `${type}; filename="${fileName.replaceAll('"', '')}"`
}

export async function getSyncthingConfig(accountId: string, userId?: string): Promise<SyncthingConfig> {
  const account = await prisma.connectedAccount.findFirstOrThrow({
    where: { id: accountId, provider: 'syncthing', status: 'connected', ...(userId ? { userId } : {}) },
  })
  // apiUrl stored in accessTokenEncrypted, apiKey in refreshTokenEncrypted
  // folderId in providerAccountId, folderPath in displayName
  const apiUrl = decryptText(account.accessTokenEncrypted!)
  const apiKey = decryptText(account.refreshTokenEncrypted!)
  const storageAccount = await prisma.storageAccount.findUnique({ where: { connectedAccountId: accountId } })
  return {
    apiUrl,
    apiKey,
    folderId: account.providerAccountId,
    folderPath: account.displayName || '/home/ubuntu/syncthing-data/9drive',
    quotaBytes: storageAccount?.totalBytes ?? null,
  }
}

function safeFileName(name: string) {
  return name.replace(/[\\/]+/g, '-').replace(/[\u0000-\u001f\u007f]+/g, '').slice(0, 180) || 'file'
}

export function buildSyncthingPath(userId: string, fileId: string, fileName: string) {
  return `${userId}/${fileId}/${safeFileName(fileName)}`
}

export async function testSyncthingConnection(apiUrl: string, apiKey: string): Promise<{ myID: string }> {
  const response = await fetch(`${apiUrl}/rest/system/status`, {
    headers: { 'X-API-Key': apiKey },
  })
  if (!response.ok) throw new Error(`Syncthing API error: ${response.status} ${response.statusText}`)
  const data = await response.json() as { myID: string }
  return data
}

export async function uploadSyncthingFile(
  config: SyncthingConfig,
  relativePath: string,
  body: NodeJS.ReadableStream,
) {
  const fullPath = join(config.folderPath, relativePath)
  await mkdir(dirname(fullPath), { recursive: true })
  await pipeline(body as Readable, createWriteStream(fullPath))

  // Trigger Syncthing rescan for the folder
  await fetch(`${config.apiUrl}/rest/db/scan?folder=${encodeURIComponent(config.folderId)}&sub=${encodeURIComponent(relativePath)}`, {
    method: 'POST',
    headers: { 'X-API-Key': config.apiKey },
  }).catch(() => undefined) // non-fatal if scan trigger fails
}

export async function deleteSyncthingFile(file: FileWithAccount) {
  const config = await getSyncthingConfig(file.connectedAccountId)
  const fullPath = join(config.folderPath, file.providerFileId)
  await unlink(fullPath).catch(() => undefined)

  // Trigger rescan
  await fetch(`${config.apiUrl}/rest/db/scan?folder=${encodeURIComponent(config.folderId)}&sub=${encodeURIComponent(file.providerFileId)}`, {
    method: 'POST',
    headers: { 'X-API-Key': config.apiKey },
  }).catch(() => undefined)
}

export async function syncSyncthingQuota(accountId: string) {
  const config = await getSyncthingConfig(accountId)

  // Calculate used bytes by walking the folder
  let usedBytes = 0n
  async function walkDir(dir: string) {
    const entries = await readdir(dir, { withFileTypes: true }).catch(() => [])
    for (const entry of entries) {
      const fullPath = join(dir, entry.name)
      if (entry.name === '.stfolder' || entry.name === '.stignore') continue
      if (entry.isDirectory()) {
        await walkDir(fullPath)
      } else if (entry.isFile()) {
        const s = await stat(fullPath).catch(() => null)
        if (s) usedBytes += BigInt(s.size)
      }
    }
  }
  await walkDir(config.folderPath)

  return prisma.storageAccount.upsert({
    where: { connectedAccountId: accountId },
    create: {
      connectedAccountId: accountId,
      totalBytes: config.quotaBytes,
      usedBytes,
      availableBytes: config.quotaBytes === null ? null : config.quotaBytes - usedBytes,
      lastSyncedAt: new Date(),
    },
    update: {
      totalBytes: config.quotaBytes,
      usedBytes,
      availableBytes: config.quotaBytes === null ? null : config.quotaBytes - usedBytes,
      lastSyncedAt: new Date(),
    },
  })
}

export async function streamSyncthingFile(file: FileWithAccount, range: string | undefined, res: Response, options: StreamOptions = {}) {
  const config = await getSyncthingConfig(file.connectedAccountId)
  const fullPath = join(config.folderPath, file.providerFileId)
  const fileStat = await stat(fullPath)

  if (range) {
    const match = range.match(/bytes=(\d+)-(\d*)/)
    if (match) {
      const start = parseInt(match[1], 10)
      const end = match[2] ? parseInt(match[2], 10) : fileStat.size - 1
      const chunkSize = end - start + 1

      res.status(206)
      res.setHeader('Content-Type', file.mimeType)
      res.setHeader('Accept-Ranges', 'bytes')
      res.setHeader('Content-Length', chunkSize.toString())
      res.setHeader('Content-Range', `bytes ${start}-${end}/${fileStat.size}`)
      if (options.disposition) res.setHeader('Content-Disposition', contentDisposition(options.disposition, file.name))

      return createReadStream(fullPath, { start, end }).pipe(res)
    }
  }

  res.status(200)
  res.setHeader('Content-Type', file.mimeType)
  res.setHeader('Accept-Ranges', 'bytes')
  res.setHeader('Content-Length', fileStat.size.toString())
  if (options.disposition) res.setHeader('Content-Disposition', contentDisposition(options.disposition, file.name))

  return createReadStream(fullPath).pipe(res)
}
