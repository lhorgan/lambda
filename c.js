var AWS = require('aws-sdk');
// Set the region 
AWS.config.update({region: 'us-east-1'});
var fs = require('fs');
const lineByLine = require('n-readlines');
var level = require('level');
const fetch = require('node-fetch');

class Earl {
    constructor(lambda_names, ifname, ofname, dbname, servers, lambdasPerServer) {
        this.lambda_names = lambda_names;
        this.ips = {};
        this.readstream = new lineByLine(ifname);
        this.db = level(dbname);
        this.allLinesRead = false;
        this.urlsToWrite = [];
        this.ofname = ofname;
        this.numProcessed = 0;
        this.servers = servers;
        this.lambdasPerServer = lambdasPerServer;
        this.queue = [];
        this.urlCount = 0;
        this.done = false;

        this.collectIPs();

        let interval = setInterval(async () => {
            this.done = !await this.expand();
            if(this.done) {
                clearInterval(interval);
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

            let name = ipList[i][1];
            let region = ipList[i][2];
            ctr++;

            urlsToSend.push([name, region, url]);
            if(urlsToSend.length >= this.lambdasPerServer || ctr === ipList.length) {
                console.log("SENDING TO " + region + " at IP " + ipList[i][0]);
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
        //console.log("Fetching from server " + server);
        //console.log(urls);
        fetch(server + "/urls", {
            method: "post",
            body: urls,
            headers: { 'Content-Type': 'application/json' }
        })
        .then(this.handleErrors)
        .then(response => response.json())
        .then(data => {
            this.writeURLs(data);
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

    writeURLs(urls) {
        //console.log("Writing a batch of URLs");
        this.urlsToWrite = [];
        let urlStr = "";
        for(let i = 0; i < urls.length; i++) {
            urlStr += urls[i].url + "\t" + 
                      urls[i].orig_url + "\t" + 
                      urls[i].time + "\t" +
                      urls[i].err + "\t" +
                      urls[i].msg;
            urlStr += "\r\n";

            this.db.put(urls[i].orig_url, "", () => {});
        }

        var stream = fs.createWriteStream(this.ofname, {flags:'a'});
        stream.write(urlStr);
        stream.end();
    }

    async getNextURL() {
        let queueURL = this.queue.pop();
        if(queueURL) {
            return [queueURL, 2020];
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
                return [null, null];
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
    for(let i = 0; i < 900; i++) {
        ln.push(["hydrate-" + i, "us-east-1"]);
        ln.push(["hydrate-" + i, "us-west-2"]);
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
                                50);
        }
    });
}

go();