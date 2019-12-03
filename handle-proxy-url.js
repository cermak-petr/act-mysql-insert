const { createTunnel } = require('proxy-chain');
const ping = require('ping');
const hostile = require('hostile');

module.exports = async (proxyUrl, host, port = '3306') => {
    const tunnel = await createTunnel(proxyUrl, `${host}:${port}`);
    console.log('Created tunnel:', tunnel);
    // add item to /etc/hosts
    console.log('Setting hostile for host:', host);
    await new Promise((resolve, reject) => {
        hostile.set('127.0.0.1', host, (err) => {
            if (err) return reject(err);
            return resolve();
        });
    });

    // Test connectivity to proxy
    const data = await ping.promise.probe(host);
    console.log('Connecting to ip', data.numeric_host);
    console.log('Host is alive', data.alive);

    const [newHost, newPort] = tunnel.replace('localhost', host).split(':');
    return {
        newHost,
        newPort,
    };
};
