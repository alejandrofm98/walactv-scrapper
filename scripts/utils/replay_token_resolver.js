const https = require('https');
const vm = require('vm');

const USER_AGENT = (
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ' +
  '(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36'
);

function requestText(url, { method = 'GET', headers = {}, body = null, cookie = '' } = {}) {
  return new Promise((resolve, reject) => {
    const target = new URL(url);
    const request = https.request(
      target,
      {
        method,
        headers: {
          'User-Agent': USER_AGENT,
          ...headers,
          ...(cookie ? { Cookie: cookie } : {}),
        },
      },
      (response) => {
        let data = '';
        response.setEncoding('utf8');
        response.on('data', (chunk) => {
          data += chunk;
        });
        response.on('end', () => {
          resolve({
            text: data,
            headers: response.headers,
            statusCode: response.statusCode || 0,
          });
        });
      }
    );

    request.on('error', reject);
    if (body) {
      request.write(body);
    }
    request.end();
  });
}

function createElement(tagName) {
  return {
    tagName: String(tagName || '').toUpperCase(),
    style: {},
    attributes: {},
    children: [],
    classList: {
      add() {},
      remove() {},
    },
    appendChild(child) {
      this.children.push(child);
      return child;
    },
    remove() {},
    addEventListener() {},
    removeEventListener() {},
    setAttribute(name, value) {
      this.attributes[name] = value;
      this[name] = value;
    },
    innerHTML: '',
    src: '',
  };
}

async function resolveProviderUrlFromToken(token, tokenEnc, refererUrl) {
  const submitUrl = `https://techradan.com/?id=${encodeURIComponent(token)}`;
  const submitBody = new URLSearchParams({ id: token, id_enc: tokenEnc }).toString();

  const initialResponse = await requestText(submitUrl, {
    method: 'POST',
    body: submitBody,
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Referer: refererUrl,
    },
  });

  const alternateFormMatch = initialResponse.text.match(/<form[^>]*action="([^"]+)"[^>]*>/i);
  let resolverBaseUrl = 'https://techradan.com';
  let submitResponse = initialResponse;

  if (!initialResponse.headers['set-cookie'] && alternateFormMatch) {
    const alternateAction = alternateFormMatch[1];
    const alternateUrl = new URL(alternateAction);
    resolverBaseUrl = `${alternateUrl.protocol}//${alternateUrl.host}`;
    submitResponse = await requestText(alternateAction, {
      method: 'POST',
      body: new URLSearchParams({ id: token }).toString(),
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Referer: submitUrl,
      },
    });
  }

  const rawCookies = submitResponse.headers['set-cookie'] || initialResponse.headers['set-cookie'] || [];
  const cookieList = Array.isArray(rawCookies) ? rawCookies : [rawCookies];
  const cookieHeader = cookieList
    .map((value) => String(value).split(';')[0])
    .join('; ');

  if (!cookieHeader) {
    throw new Error('No se obtuvieron cookies desde el resolvedor');
  }

  const playerUrl = `${resolverBaseUrl}/2024_oct/embed_player_provider`;
  const preloadScript = (
    await requestText(`${resolverBaseUrl}/2024_oct/cached_data_provider.php`, {
      headers: { Referer: playerUrl },
      cookie: cookieHeader,
    })
  ).text;

  const loaderScript = (
    await requestText(`${resolverBaseUrl}/robots_.js?ver=3.28`, {
      headers: { Referer: playerUrl },
      cookie: cookieHeader,
    })
  ).text;

  const cryptoScript = (
    await requestText('https://cdnjs.cloudflare.com/ajax/libs/crypto-js/3.1.2/rollups/aes.js')
  ).text;

  const cutIndex = loaderScript.indexOf('script_loader()[');
  const trimmedLoader = cutIndex > 0 ? loaderScript.slice(0, cutIndex) : loaderScript;

  const body = {
    style: {},
    innerHTML: '',
    appendChild() {},
    removeChild() {},
  };
  const head = {
    appendChild() {},
    removeChild() {},
  };

  const document = {
    body,
    head,
    documentElement: {},
    createElement,
    querySelector(selector) {
      if (selector === 'body') {
        return body;
      }
      if (selector === 'head') {
        return head;
      }
      return body;
    },
    getElementsByTagName(tagName) {
      if (tagName === 'head') {
        return [head];
      }
      if (tagName === 'body') {
        return [body];
      }
      return [];
    },
    addEventListener() {},
    removeEventListener() {},
    styleSheets: [{ insertRule() {}, addRule() {}, cssRules: [] }],
  };

  const context = vm.createContext({
    window: {},
    globalThis: {},
    document,
    console,
    navigator: { userAgent: USER_AGENT },
    location: {
      href: playerUrl,
      pathname: new URL(playerUrl).pathname,
      search: new URL(playerUrl).search,
    },
    atob(value) {
      return Buffer.from(value, 'base64').toString('binary');
    },
    btoa(value) {
      return Buffer.from(value, 'binary').toString('base64');
    },
    setTimeout() {},
    clearTimeout() {},
    setInterval() {},
    clearInterval() {},
    Math,
    Date,
    JSON,
    Array,
    Object,
    String,
    Number,
    Boolean,
    RegExp,
    parseInt,
    parseFloat,
    isNaN,
    encodeURIComponent,
    decodeURIComponent,
  });

  context.window = context;
  context.globalThis = context;

  vm.runInContext(preloadScript, context, { timeout: 10000 });
  vm.runInContext(cryptoScript, context, { timeout: 10000 });
  vm.runInContext(trimmedLoader, context, { timeout: 10000 });

  const resolver = vm.runInContext('Q3J5cHRvSl()', context, { timeout: 10000 });
  return resolver && resolver.decrypted_url ? resolver.decrypted_url : '';
}

async function main() {
  const token = process.argv[2];
  const tokenEnc = process.argv[3];
  const refererUrl = process.argv[4] || 'https://watch-wrestling.eu/';

  if (!token || !tokenEnc) {
    process.stderr.write('Uso: node replay_token_resolver.js <token> <token_enc> [referer_url]\n');
    process.exit(1);
  }

  try {
    const providerUrl = await resolveProviderUrlFromToken(token, tokenEnc, refererUrl);
    process.stdout.write(providerUrl || '');
  } catch (error) {
    process.stderr.write(`${error.message}\n`);
    process.exit(1);
  }
}

main();
