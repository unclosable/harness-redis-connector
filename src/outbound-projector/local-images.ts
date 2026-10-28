/** A local markdown image candidate, including its original source span. */
export interface LocalImageReference {
  readonly markdown: string
  readonly destination: string
  readonly start: number
  readonly end: number
}

export interface ExtractedLocalImages {
  readonly references: readonly LocalImageReference[]
  /** Text with every eligible local-reference markdown span removed. */
  readonly text: string
}

/**
 * Extract local image markdown references without interpreting the filesystem.
 * Remote/application destinations are deliberately left in the visible text.
 */
export function extractLocalImageReferences(text: string): ExtractedLocalImages {
  const references: LocalImageReference[] = []
  // The destination form emitted by deliverable-file-references is deliberately
  // simple. This also accepts a quoted title after a plain destination.
  const image = /!\[[^\]]*\]\(\s*(?:<([^>]+)>|([^\s)]+))(?:\s+(?:"[^"]*"|'[^']*'))?\s*\)/g
  for (const segment of markdownSegments(text)) {
    image.lastIndex = segment.start
    for (let match = image.exec(text); match !== null && match.index < segment.end; match = image.exec(text)) {
      // A match cannot cross a protected segment: its first character is in this
      // segment and `](` cannot occur in a code segment which we never scan.
      if (match.index + match[0].length > segment.end) continue
      const raw = (match[1] ?? match[2] ?? '').trim()
      const destination = decodeDestination(raw)
      if (destination === undefined || isNonLocalDestination(destination)) continue
      references.push({ markdown: match[0], destination, start: match.index, end: match.index + match[0].length })
    }
  }
  return { references, text: removeLocalImageReferences(text, references) }
}

/** Remove selected references and collapse only blank-line runs introduced by it. */
export function removeLocalImageReferences(text: string, references: readonly LocalImageReference[]): string {
  if (references.length === 0) return text
  const marker = '\u0000'
  let result = ''
  let offset = 0
  for (const reference of references) {
    const lineStart = text.lastIndexOf('\n', reference.start - 1) + 1
    const lineEndIndex = text.indexOf('\n', reference.end)
    const lineEnd = lineEndIndex === -1 ? text.length : lineEndIndex
    const isOwnLine = /^[ \t]*$/.test(text.slice(lineStart, reference.start)) && /^[ \t]*$/.test(text.slice(reference.end, lineEnd))
    const start = isOwnLine ? lineStart : reference.start
    const end = isOwnLine ? lineEnd : reference.end
    result += text.slice(offset, start)
    if (isOwnLine) {
      // Keep a sentinel until all removals are known, so only blank lines made
      // adjacent by this removal are collapsed (not pre-existing blank runs).
      result += marker
    } else if (result.length === 0 && start === 0 && /^[ \t]/.test(text.slice(end))) {
      // At the start of text, the following run only separated the image.
      let next = end
      while (text[next] === ' ' || text[next] === '\t') next += 1
      offset = next
      continue
    } else if (/[ \t]$/.test(result) && /^[ \t]/.test(text.slice(end))) {
      // Inline image removal joins two horizontal whitespace runs; retain one.
      let next = end
      while (text[next] === ' ' || text[next] === '\t') next += 1
      offset = next
      continue
    } else if (/[ \t]$/.test(result) && end === text.length && /\S[ \t]+$/.test(result)) {
      // The sole preceding space was an inline separator, not trailing prose.
      result = result.replace(/[ \t]+$/, '')
    }
    offset = end
  }
  result += text.slice(offset)
  const cleaned = result
    .replace(/\n(?:[ \t]*\n)*[ \t]*\u0000(?:\n[ \t]*)+/g, '\n\n')
    .replaceAll(marker, '')
  return /^[ \t]*$/.test(cleaned) ? '' : cleaned
}

interface Segment { start: number; end: number }

/** Return only prose spans; fenced and inline code are preserved byte-for-byte. */
function markdownSegments(text: string): Segment[] {
  const protectedRanges: Segment[] = []
  const fence = /(^|\n)[ \t]*(`{3,}|~{3,})[^\n]*(?:\n|$)/g
  for (let opening = fence.exec(text); opening !== null; opening = fence.exec(text)) {
    const delimiter = opening[2]!
    const close = new RegExp(`(^|\\n)[ \\t]*${delimiter[0] === '`' ? '`' : '~'}{${delimiter.length},}[ \\t]*(?=\\n|$)`, 'g')
    close.lastIndex = opening.index + opening[0].length
    const closing = close.exec(text)
    const end = closing === null ? text.length : closing.index + closing[0].length
    protectedRanges.push({ start: opening.index, end })
    fence.lastIndex = end
  }
  let cursor = 0
  while (cursor < text.length) {
    if (protectedRanges.some(range => cursor >= range.start && cursor < range.end)) {
      cursor = protectedRanges.find(range => cursor >= range.start && cursor < range.end)!.end
      continue
    }
    if (text[cursor] !== '`') { cursor += 1; continue }
    let length = 1
    while (text[cursor + length] === '`') length += 1
    const end = matchingBacktickRun(text, cursor + length, length)
    if (end !== undefined) {
      protectedRanges.push({ start: cursor, end: end + length })
      cursor = end + length
    } else cursor += length
  }
  protectedRanges.sort((a, b) => a.start - b.start)
  const result: Segment[] = []
  cursor = 0
  for (const range of protectedRanges) {
    if (cursor < range.start) result.push({ start: cursor, end: range.start })
    cursor = Math.max(cursor, range.end)
  }
  if (cursor < text.length) result.push({ start: cursor, end: text.length })
  return result
}

/** A longer backtick run is text here, never the closer for a shorter opener. */
function matchingBacktickRun(text: string, start: number, length: number): number | undefined {
  for (let index = start; index < text.length; index += 1) {
    if (text[index] !== '`') continue
    let runLength = 1
    while (text[index + runLength] === '`') runLength += 1
    if (runLength === length) return index
    index += runLength - 1
  }
  return undefined
}

function decodeDestination(value: string): string | undefined {
  try {
    return decodeURIComponent(value).replace(/^file:\/\//i, '')
  } catch {
    return undefined
  }
}

function isNonLocalDestination(destination: string): boolean {
  return /^(?:https?:|data:|dsh-app:)/i.test(destination)
}
