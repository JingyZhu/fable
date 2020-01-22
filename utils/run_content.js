/* 
    Run the chrome to load the page
    Usage: node run.js url --filename filename --screenshot
    filename and screenshot is optional
    Save the chrome's pid into filename.html for easy killing
    If screenshot is eanbled, save the screenshot as filename.jpg in Base64!! Require decoding to see
*/

const CDP = require('chrome-remote-interface');
const fs = require('fs')
const parse = require('url').parse
const chromeLauncher = require('chrome-launcher');
const assert = require('assert');
const argv = require('yargs').argv;



async function writeHTML(Runtime, filename) {
    const result = await Runtime.evaluate({
        expression: 'document.documentElement.outerHTML'
    });
    const html = result.result.value;
    fs.writeFileSync(filename , html);
}

async function startChrome(){
    const os = process.platform;
    assert(os == 'linux' | os == 'darwin')
    const path = os == 'linux' ? '/opt/google/chrome/chrome' : '/Applications/Chromium.app/Contents/MacOS/Chromium'
    
    let chromeFlags = [
        '--disk-cache-size=1', 
        '-disable-features=IsolateOrigins,site-per-process',
    ];
    
    if (os == 'linux') chromeFlags.push('--headless')
    const chrome = await chromeLauncher.launch({
        chromeFlags: chromeFlags,
        chromePath: path,
        // userDataDir: '/tmp/nonexistent' + Date.now(), 
    })
    return chrome;
}


(async function(){
    const chrome = await startChrome();
    let filename = argv.filename != undefined ? argv.filename : "temp"
    htmlname = `${filename}.html`;

    let screenshot = argv.screenshot; 
    
    fs.writeFileSync(htmlname, chrome.pid);
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

        let obj = await Runtime.evaluate({expression: 'org.chromium.distiller.DomDistiller.apply()[2][1]'})

        console.log(obj)

        await writeHTML(Runtime, htmlname);

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