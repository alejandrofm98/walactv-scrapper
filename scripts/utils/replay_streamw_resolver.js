const https = require('https');

function fetchText(url) {
  return new Promise((resolve, reject) => {
    https.get(
      url,
      {
        headers: {
          'User-Agent': (
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ' +
            '(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36'
          ),
        },
      },
      (res) => {
        let data = '';
        res.setEncoding('utf8');
        res.on('data', (chunk) => {
          data += chunk;
        });
        res.on('end', () => resolve(data));
      }
    ).on('error', reject);
  });
}

function normalizeUrl(url) {
  return url.replace('https://hglink.to/e/', 'https://vibuxer.com/e/');
}

function unpackPackerScript(html) {
  const lowered = html.toLowerCase();
  if (
    lowered.includes('file is no longer available') ||
    lowered.includes('has been deleted') ||
    lowered.includes('expired or has been deleted')
  ) {
    return '';
  }

  const match = html.match(/eval\(function\(p,a,c,k,e,d\)[\s\S]*?<\/script>/i);
  if (!match) {
    throw new Error('No se encontro script packer de StreamW');
  }

  const packed = match[0].replace(/<\/script>\s*$/i, '');
  const unpackedExpression = packed.replace(
    'eval(function(p,a,c,k,e,d)',
    '(function(p,a,c,k,e,d)'
  );

  return eval(unpackedExpression);
}

function extractBestStream(unpackedScript, pageUrl) {
  const baseUrl = new URL(pageUrl);
  const candidates = [];

  const relativeMatches = unpackedScript.match(/\/stream\/[^"'\s]+master\.m3u8[^"'\s]*/g) || [];
  for (const match of relativeMatches) {
    candidates.push(new URL(match, baseUrl).toString());
  }

  const absoluteMatches = unpackedScript.match(/https?:\/\/[^"'\s]+\.m3u8[^"'\s]*/g) || [];
  for (const match of absoluteMatches) {
    candidates.push(match);
  }

  const unique = [...new Set(candidates)];
  if (!unique.length) {
    return '';
  }

  const preferred = unique.find((url) => url.includes('/master.m3u8'));
  return preferred || unique[0];
}

async function resolveStreamWStream(pageUrl) {
  const normalizedUrl = normalizeUrl(pageUrl);
  const html = await fetchText(normalizedUrl);
  const unpackedScript = unpackPackerScript(html);
  if (!unpackedScript) {
    return '';
  }
  return extractBestStream(unpackedScript, normalizedUrl);
}

async function main() {
  const pageUrl = process.argv[2];
  if (!pageUrl) {
    process.stderr.write('Uso: node replay_streamw_resolver.js <provider_url>\n');
    process.exit(1);
  }

  try {
    const streamUrl = await resolveStreamWStream(pageUrl);
    process.stdout.write(streamUrl || '');
  } catch (error) {
    process.stderr.write(`${error.message}\n`);
    process.exit(1);
  }
}

main();
