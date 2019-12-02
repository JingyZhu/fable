const CDP = require('chrome-remote-interface');
const fs = require('fs')
const chromeLauncher = require('chrome-launcher');
const assert = require('assert');
const prompts = require('prompts');

const ports = [9222, 9223];

const getLine = async str=>{
    const response = await prompts({
        type: 'text',
        name: 'value',
        message: str
    })
    return response.value
}

async function startChrome(){
    const os = process.platform;
    assert(os == 'linux' | os == 'darwin')
    const path = os == 'linux' ? '/opt/google/chrome/chrome' : '/Applications/Chromium.app/Contents/MacOS/Chromium'
    const chrome = await chromeLauncher.launch({
        port: Number(ports[process.argv[2]]),
        chromeFlags: [
            // '--headless',
            '--disable-gpu', 
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
    console.log(chrome.port);
    await getLine('Finished?');
    await chrome.kill();
})()