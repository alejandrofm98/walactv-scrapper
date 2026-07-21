const https = require('https');
const vm = require('vm');

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

async function resolveProviderUrl(embedUrl) {
  const embedHtml = await fetchText(embedUrl);
  const scriptMatches = [...embedHtml.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/gi)];
  if (scriptMatches.length < 2) {
    throw new Error('No se encontraron scripts inline en el embed');
  }

  const preloadScript = scriptMatches[1][1];
  const loaderScript = await fetchText('https://dailywrestling.cc/robots_.js?ver=3.27');
  const cryptoScript = await fetchText(
    'https://cdnjs.cloudflare.com/ajax/libs/crypto-js/3.1.2/rollups/aes.js'
  );

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
    navigator: {
      userAgent: (
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ' +
        '(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36'
      ),
    },
    location: {
      href: embedUrl,
      pathname: new URL(embedUrl).pathname,
      search: new URL(embedUrl).search,
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
  const embedUrl = process.argv[2];
  if (!embedUrl) {
    process.stderr.write('Uso: node replay_embed_resolver.js <embed_url>\n');
    process.exit(1);
  }

  try {
    const providerUrl = await resolveProviderUrl(embedUrl);
    process.stdout.write(providerUrl || '');
  } catch (error) {
    process.stderr.write(`${error.message}\n`);
    process.exit(1);
  }
}

main();
