const CDP = require('chrome-remote-interface');
const fs = require('fs')
const parse = require('url').parse
const chromeLauncher = require('chrome-launcher');
const assert = require('assert');

async function writeHTML(Runtime) {
    const result = await Runtime.evaluate({
        expression: 'document.documentElement.outerHTML'
    });
    const html = result.result.value;
    let filename = process.argv.length > 3 ? process.argv[3] : 'temp';
    filename = `${filename}.html`;
    fs.writeFileSync(filename  , html);
}

async function startChrome(){
    const os = process.platform;
    assert(os == 'linux' | os == 'darwin')
    const path = os == 'linux' ? '/opt/google/chrome/chrome' : '/Applications/Chromium.app/Contents/MacOS/Chromium'
    const chrome = await chromeLauncher.launch({
        chromeFlags: [
            // '--headless',
            // '--disable-gpu', 
            '--ignore-certificate-errors',
            '--disk-cache-size=1', 
            '-disable-features=IsolateOrigins,site-per-process',
        ],
        chromePath: path,
        // userDataDir: '/tmp/nonexistent' + Date.now(), 
    })
    return chrome;
}


(async function(){
    const chrome = await startChrome();
    let filename = process.argv.length > 3 ? process.argv[3] : 'temp';
    filename = `${filename}.html`;
    fs.writeFileSync(filename, chrome.pid);
    const client = await CDP({port: chrome.port});
    const { Network, Page, Security, Runtime} = client;
    // console.log(Security);

    try {
        await Security.setIgnoreCertificateErrors({ ignore: true });
        //Security.disable();

        await Network.enable();
        await Page.enable();

        await Page.navigate({ url: process.argv[2] });

        await Page.loadEventFired();

        await writeHTML(Runtime);

    } catch (err) {
        console.error(err);
    } finally {
        if (client){
            client.close();
            await chrome.kill();
            process.exit(0);
        }
    }

})()