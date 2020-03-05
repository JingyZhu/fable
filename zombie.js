const Zombie = require('zombie');
const Browser = require('../browser');
const setCookie = require('set-cookie-parser')


class ZombieBrowser extends Browser {
  constructor(options) {
    super(options);

    this.browser = new Zombie({
      proxy: options.proxy,
      silent: true,
      strictSSL: false,
      userAgent: options.userAgent,
      waitDuration: options.maxWait,
    });

    this.browser.on('authenticate', (auth) => {
      auth.username = this.options.username;
      auth.password = this.options.password;
    });

    // jingyz: Added for detect tech used from wayback
    this.browser.pipeline.addHandler(function(browser, request, response){
      if (!response._url.includes('web.archive.org')) return response
      let new_headers = [];
      for (let li in response.headers._headers){
        let [k, v] = response.headers._headers[li];
        if(k.match(/x-archive-orig/gi)) {
          let realHeader = k.toLowerCase().replace('x-archive-orig-', '')
          new_headers.push([realHeader, v])
          if (realHeader == 'set-cookie'){ // Mannually set the cookie for x-archive-orig-set-cookie
            const wayback_cookie = setCookie.parse(v);
            for (let idx in wayback_cookie){
              const name = wayback_cookie[idx].name;
              const value = wayback_cookie[idx].value;
              const domain =  'domain' in wayback_cookie ? wayback_cookie.domain : new URL(response._url).host;
              const path =  'path' in wayback_cookie ? wayback_cookie.path : '/';
              browser.setCookie({
                name: name,
                value: value,
                domain: domain,
                path: path
              })
            }
          }
        }
      } 
      response.headers._headers = new_headers;
      return response
    });

  }

  visit(url) {
    return new Promise((resolve, reject) => {
      try {
        this.browser.visit(url, () => {
          const resource = this.browser.resources.length
            ? this.browser.resources.filter(_resource => _resource.response).shift() : null;
  
          this.window = this.browser.window;
          this.document = this.browser.document;
          this.headers = this.getHeaders();
          this.statusCode = resource ? resource.response.status : 0;
          this.contentType = this.headers['content-type'] ? this.headers['content-type'].shift() : null;
          this.html = this.getHtml();
          this.js = this.getJs();
          this.links = this.getLinks();
          this.scripts = this.getScripts();
          this.cookies = this.getCookies();
    

          resolve();
          
        });
      } catch (error) {
        reject(error.message);
      }
    });
  }

  getHeaders() {
    const headers = {};

    const resource = this.browser.resources.length
      ? this.browser.resources.filter(_resource => _resource.response).shift() : null;

    if (resource) {
      // eslint-disable-next-line no-underscore-dangle
      resource.response.headers._headers.forEach((header) => {
        if (!headers[header[0]]) {
          headers[header[0]] = [];
        }

        headers[header[0]].push(header[1]);
      });
    }

    return headers;
  }

  getHtml() {
    let html = '';

    if (this.browser.document && this.browser.document.documentElement) {
      try {
        html = this.browser.html();
      } catch (error) {
        this.log(error.message, 'error');
      }
    }

    return html;
  }

  getScripts() {
    let scripts = [];

    if (this.browser.document && this.browser.document.scripts) {
      scripts = Array.prototype.slice
        .apply(this.browser.document.scripts)
        .filter(script => script.src)
        .map(script => script.src);
    }

    return scripts;
  }

  getJs() {
    return this.browser.window;
  }

  getLinks() {
    let links = [];

    if (this.browser.document) {
      links = Array.from(this.browser.document.getElementsByTagName('a'));
    }

    return links;
  }

  getCookies() {
    const cookies = [];

    if (this.browser.cookies) {
      this.browser.cookies.forEach(cookie => cookies.push({
        name: cookie.key,
        value: cookie.value,
        domain: cookie.domain,
        path: cookie.path,
      }));
    }

    return cookies;
  }
}

module.exports = ZombieBrowser;
