const CDP = require('chrome-remote-interface');

(async function (){
    const port = Number(process.argv[3]);
    const client = await CDP({port: port});
    const { Network, Page, Security, Runtime} = client;
    // console.log(Security);

    try {
        await Security.setIgnoreCertificateErrors({ ignore: true });
        //Security.disable();

        await Network.enable();
        await Page.enable();

        await Page.navigate({ url: process.argv[2] });

        await Page.loadEventFired();

    } catch (err) {
        console.error(err);
    } finally {
        client.close();
    }
})()