var AWS = require('aws-sdk');
// Set the region 
AWS.config.update({region: 'us-east-1'});
var fs = require('fs');
const lineByLine = require('n-readlines');
var level = require('level');
const fetch = require('node-fetch');
var Url = require('url-parse');

class Earl {
    constructor(lambda_names, ifname, ofname, dbname, servers, specialMethods, lambdasPerServer) {
        this.lambda_names = lambda_names;
        this.ips = {};
        this.readstream = new lineByLine(ifname);
        this.db = level(dbname);
        this.allLinesRead = false;
        //this.totalLines = 0;
        this.urlsToWrite = [];
        this.ofname = ofname;
        this.numProcessed = 0;
        this.servers = servers;
        this.lambdasPerServer = lambdasPerServer;
        this.queue = [];
        this.urlCount = 0;
        this.urlWriteCount = 0;
        this.done = false;

        this.specialMethods = specialMethods;
        this.timeouts = {};
        this.successCounts = {};
        this.specialTimeouts = {};
        this.retries = {};
        this.hopeless = {};

        this.collectIPs();
        let queueEmpty = this.queue.length === 0;

        let timeoutInterval = setInterval(() => {
            let numInPurgatory = 0;
            console.log("Assessing our timeout interval");
            for(let domain in this.timeouts) {
                let successCounts = 0;
                if(domain in this.successCounts) {
                    successCounts = this.successCounts[domain];
                }
                let total = successCounts + this.timeouts[domain].length;
                console.log(`Total successes for ${domain} is ${successCounts}, against total attempts: ${total}`);
                numInPurgatory += this.timeouts[domain].length;

                if(total >= 6 || (queueEmpty && this.allLinesRead)) {
                    console.log("Enough requests have been made for us to pass judgement");
                    if(successCounts / total < 0.67) {
                        console.log(`Enough requests to ${domain} failed for us to bump up its timeout`);
                        let currentTimeout = 3.5;
                        if(domain in this.specialTimeouts) {
                            currentTimeout = this.specialTimeouts[domain];
                        }
                        let newTimeout = currentTimeout * 2;
                        console.log(`New timeout for ${domain} is ${newTimeout}`);
                        if(newTimeout < 30) {
                            this.specialTimeouts[domain] = newTimeout;
                            this.queue = this.queue.concat(this.timeouts[domain]);
                            delete this.timeouts[domain];
                        }
                        else {
                            console.log(`The timeout is too high, ${domain} is hopeless.`);
                            // this url is hopeless
                            this.hopeless[domain] = true;
                            delete this.specialTimeouts[domain];
                            this.queue = this.queue.concat(this.timeouts[domain]);
                            delete this.timeouts[domain];
                        }
                    }
                    else {
                        console.log(`${domain} is doing pretty okay at its current timeout`);
                        // we have enough succeses as things are
                        delete this.successCounts[domain];
                        this.queue = this.queue.concat(this.timeouts[domain]);
                        delete this.timeouts[domain];
                    }
                }
            }
            console.log("We now have " + numInPurgatory + " urls in purgatory");
        }, 20000);

        let interval = setInterval(async () => {
            this.done = !await this.expand();
            if(this.done) {
                clearInterval(interval);
                clearInterval(timeoutInterval);
                console.log("That's all, folks.");
            }
        }, 1000);
    }

    async expand() {
        let ipFreeze = JSON.parse(JSON.stringify(this.ips));
        
        let serverIndex = 0;
        let urlsToSend = [];
        let ctr = 0;
        //let numIPs = Object.keys(ipFreeze).length;
        
        let ipList = [];
        for(let ip in ipFreeze) {
            ipList.push([ip, ipFreeze[ip][0], ipFreeze[ip][1]]); // [ip, name, region]
        }
        shuffle(ipList);

        for(let i = 0; i < ipList.length; i++) {
            let [url, year] = await this.getNextURL();

            if(url === null && year === null) {
                let urlsToSendFreeze = JSON.stringify(urlsToSend);
                this.sendURLs(this.servers[serverIndex], urlsToSendFreeze);
                urlsToSend = [];

                return false;
            }
            /*else if(url === null) {
                return true;
            }*/

            if(url !== null) {
                let name = ipList[i][1];
                let region = ipList[i][2];
                
                let special = {};
                let urlObj = new Url(url);
                let domain = urlObj.host;
                if(domain in this.specialTimeouts) {
                    special["timeout"] = this.specialTimeouts[domain];
                }
                if(domain in this.specialMethods) {
                    special["method"] = this.specialMethods[domain];
                }
                urlsToSend.push([name, region, url, special]);
            }
            ctr++;

            if(urlsToSend.length >= this.lambdasPerServer || ctr === ipList.length) {
                let urlsToSendFreeze = JSON.stringify(urlsToSend);
                this.sendURLs(this.servers[serverIndex], urlsToSendFreeze);
                urlsToSend = [];

                serverIndex++;
                if(serverIndex >= this.servers.length) {
                    break;
                }
            }
        }

        return true;
    }

    handleErrors(response) {
        if(!response.ok) {
            throw Error(response.statusText);
        }
        return response;
    }

    sendURLs(server, urls) {
        //console.log("SENDING URLS WITH LENGTH " + urls.length);
        //console.log(JSON.stringify(urls));
        //console.log(urls[0]);
        if(urls.length === 2) { // "[]"
            console.log("LENGTH IS 0, skipping");
            return;
        }

        fetch(server + "/urls", {
            method: "post",
            body: urls,
            headers: { 'Content-Type': 'application/json' }
        })
        .then(this.handleErrors)
        .then(response => response.json())
        .then(data => {
            let processedURLs = this.processURLs(data);
            this.writeURLs(processedURLs);
            this.numProcessed += data.length;
            console.log(this.numProcessed);
        })
        .catch(err => {
            console.log(err);
            console.log("Something seems to have gone wrong...");
            urls = JSON.parse(urls);
            for(let i = 0; i < urls.length; i++) {
                this.queue.push(urls[i][2]);
            }
        });
    }

    processURLs(urls) {
        let urlsEnqueuedStart = this.queue.length;
        let urlsToWriteStart = urls.length;
        let urlsTimedOutStart = 0;
        for(let domain in this.timeouts) {
            urlsTimedOutStart += this.timeouts[domain].length;
        }

        let urlsToWrite = [];
        for(let i = 0; i < urls.length; i++) {
            let url = urls[i].orig_url;
            console.log(url + " --> " + urls[i].url);
            let urlObj = new Url(url);
            let domain = urlObj.host;
            if(urls[i].err === "true") {
                console.log("Error on " + url);
                if(urls[i].msg === "<class 'urllib3.exceptions.MaxRetryError'>") {
                    console.log("Error type: timeout");
                    if(domain in this.hopeless) {
                        console.log(`${domain} is hopeless`);
                        if(url in this.retries) {
                            urls[i].retries = this.retries[url];
                        }
                        else {
                            urls[i].retries = 0;
                        }
                        urls[i].timeout = "MAX";
                        urlsToWrite.push(urls[i]);
                    }
                    else {
                        console.log(`${url} time out but not hopeless, adding to timeout list for ${domain}`)
                        if(!(domain in this.timeouts)) {
                            this.timeouts[domain] = [];
                        }
                        this.timeouts[domain].push(url);
                    }
                }
                else {
                    console.log("Another type of error occurred on " + url);
                    if(!(url in this.retries)) {
                        this.retries[url] = 0;
                    }
                    
                    if(this.retries[url] < 4) {
                        this.retries[url]++;
                        console.log(`Giving ${url} another try, attempts now at ${this.retries[url]}`);
                        this.queue.push(url);
                    }
                    else {
                        console.log("Max retries exceeded for " + url);
                        delete this.retries[url];
                        if(urls[i].url === "null" || urls[i].url === null) {
                            urls[i].url = url;
                        }
                        urls[i].retries = "MAX";
                        urls[i].timeout = 3.5;
                        if(url in this.specialTimeouts) {
                            urls[i].timeout = this.specialTimeouts;
                        }
                        urlsToWrite.push(urls[i]);
                    }
                }
            }
            else {
                if(urls[i].url === null || urls[i].url === "null" || urls[i].url === undefined) {
                    console.log("No URL returned for " + url);
                    urls[i].url = url;
                    if(!(url in this.retries)) {
                        this.retries[url] = 0;
                    }
                    
                    if(this.retries[url] < 4) {
                        this.retries[url]++;
                        console.log(`Giving ${url} another try, attempts now at ${this.retries[url]}`);
                        this.queue.push(url);
                    }
                    else {
                        console.log("Max retries exceeded for " + url);
                        delete this.retries[url];
                        urls[i].retries = "MAX";
                        urls[i].timeout = 3.5;
                        if(url in this.specialTimeouts) {
                            urls[i].timeout = this.specialTimeouts;
                        }
                        urlsToWrite.push(urls[i]);
                    }
                }
                else {
                    //console.log("Successfully expanded " + url + " to ");
                    if(domain in this.timeouts) {
                        if(!(domain in this.successCounts)) {
                            this.successCounts[domain] = 0;
                        }
                        this.successCounts[domain]++;
                    }

                    if(url in this.retries) {
                        urls[i].retries = this.retries[url];
                    }
                    else {
                        urls[i].retries = 0;
                    }
                    urls[i].timeout = 3.5;
                    if(url in this.specialTimeouts) {
                        urls[i].timeout = this.specialTimeouts;
                    }
                    delete this.retries[url];
                    urlsToWrite.push(urls[i]);
                }
            }
        }

        let urlsEnqueuedEnd = this.queue.length;
        let urlsToWriteEnd = urlsToWrite.length;
        let urlsTimedOutEnd = 0;
        for(let domain in this.timeouts) {
            urlsTimedOutEnd += this.timeouts[domain].length;
        }

        console.log(`ENQUEUED: (${urlsEnqueuedStart}, ${urlsEnqueuedEnd}), TOWRITE: (${urlsToWriteStart}, ${urlsToWriteEnd}), TIMED OUT: (${urlsTimedOutStart}, ${urlsTimedOutEnd}), TOTAL: (${urlsEnqueuedStart + urlsToWriteStart + urlsTimedOutStart}, ${urlsEnqueuedEnd + urlsToWriteEnd + urlsTimedOutEnd})`);

        return urlsToWrite;
    }

    writeURLs(urls) {
        //console.log("Writing a batch of URLs");
        this.urlsToWrite = [];
        let urlStr = "";
        for(let i = 0; i < urls.length; i++) {
            let newURL = urls[i].url;
            if(!newURL) {
                newURL = urls[i].orig_url;
            }

            urlStr += newURL + "\t" + 
                      urls[i].orig_url + "\t" + 
                      urls[i].time + "\t" +
                      urls[i].err + "\t" +
                      urls[i].msg + "\t" +
                      urls[i].timeout + "\t" +
                      urls[i].retries
            urlStr += "\r\n";

            this.db.put(urls[i].orig_url, "", () => {});
        }
        
        this.urlWriteCount += urls.length;
        var stream = fs.createWriteStream(this.ofname, {flags:'a'});
        stream.write(urlStr);
        stream.end();
    }

    async getNextURL() {
        if(this.allLinesRead || Math.random() < 0.1) { // read an errored URL off the list with 10% probability
            let queueURL = this.queue.pop();
            if(queueURL) {
                return [queueURL, 2020];
            }
            else if(this.allLinesRead) {
                if(Object.keys(this.timeouts).length === 0 && this.urlCount === this.urlWriteCount) {
                    console.log("Not waiting on any timeouts");
                    return [null, null];
                }
                else {
                    //console.log("All lines read, queue empty, but still waiting on some timeouts!");
                    return [null, 2020];
                }
            }
        }

        while(true) { // this'll just keep going 'til we find something that isn't in the database
            let line = this.readstream.next();

            if(line) {
                this.urlCount++;
                
                if(this.urlCount % 1000 === 0) {
                    console.log("URL Count: " + this.urlCount);
                }

                line =  line.toString("utf-8");
                let [url, year] = line.trim().split("\t");

                let inDB = await this.db.get(url)
                                .then((value) => { return true })
                                .catch((err) => { return false });
                
                if(!inDB) {
                    if(!year) {
                        year = 2020;
                    }
                    return [url, year];
                }
                else {
                    //console.log("We already processed " + url);
                }
            }
            else {
                if(this.allLinesRead === false) {
                    console.log("End of file reached!");
                }
                this.allLinesRead = true;
                return [null, 2020];
            }
        }
    }

    collectIPs() {
        let ips = {};
        let numProcessed = 0;
    
        for(let i = 0; i < this.lambda_names.length; i++) {
            let name = this.lambda_names[i][0];
            let region = this.lambda_names[i][1];

            AWS.config.update({region: region});

            var params = {
                FunctionName: name,
                Payload: JSON.stringify({"ip": true})
            };
            
            var lambda = new AWS.Lambda();
            lambda.invoke(params, (err, data) => {
                numProcessed++;
                if(err) {
                    console.log(err);
                    console.log("Something went wrong");
                } // an error occurred
                else {
                    console.log(data);
                    if("Payload" in data) {
                        let payload = JSON.parse(data["Payload"]);
                        if("ip" in payload) {
                            let ip = payload["ip"].trim();
                            ips[ip] = [name, region];
                        }
                        else {
                            console.log("Payload present, but  missing IP");
                        }
                    }
                    else {
                        console.log("No payload for ip");
                    }
                }
    
                if(numProcessed === this.lambda_names.length) {
                    console.log("All " + Object.keys(ips).length + " unique IPs have been processed");
                    this.ips = ips;
                    
                    setTimeout(() => {
                        if(!this.done) {
                            console.log("Collecting ips");
                            this.collectIPs();
                        }
                    }, 3*60*1000);
                }
            });
        }
    }
}

function handleTimeout(url) {
    let urlObj = URL(url);

}

// lazy
// https://medium.com/@nitinpatel_20236/how-to-shuffle-correctly-shuffle-an-array-in-javascript-15ea3f84bfb
function shuffle(array) {
    for(let i = array.length - 1; i > 0; i--){
        const j = Math.floor(Math.random() * i);
        const temp = array[i]
        array[i] = array[j];
        array[j] = temp;
    }
}


function getLambdaNames() {
    let ln = [];
    for(let i = 0; i < 250; i++) {
        ln.push(["hydrate-" + i, "us-east-1"]);
        //ln.push(["hydrate-" + i, "us-west-2"]);
    }
    return ln;
}

function go() {
    fs.readFile("client_config.json", function(err, config) {
        if(err) {
            throw err;
        }
        else {
            config = JSON.parse(config);

            let earl = new Earl(getLambdaNames(), 
                                config.src, 
                                config.dst, 
                                config.db, 
                                config.servers, 
                                config.specialMethods,
                                50);
        }
    });
}

go();