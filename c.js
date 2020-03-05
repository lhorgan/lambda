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

        this.collectIPs();

        setInterval(async () => {
            await this.expand();
        }, 1000);
    }

    async expand() {
        let ipFreeze = JSON.parse(JSON.stringify(this.ips));
        
        let serverIndex = 0;
        let urlsToSend = [];
        let ctr = 0;
        let numIPs = Object.keys(ipFreeze).length;

        for(let ip in ipFreeze) {
            let [url, year] = await this.getNextURL();
            let name = ipFreeze[ip][0];
            let region = ipFreeze[ip][1];
            ctr++;

            urlsToSend.push([name, region, url]);
            if(urlsToSend.length >= this.lambdasPerServer || ctr === numIPs) {
                let urlsToSendFreeze = JSON.stringify(urlsToSend);
                this.sendURLs(this.servers[serverIndex], urlsToSendFreeze);
                urlsToSend = [];

                serverIndex++;
                if(serverIndex >= this.servers.length) {
                    break;
                }
            }
        }
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
                    console.log("We already processed " + url);
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
                    console.log("Something went wrong");
                } // an error occurred
                else {
                    console.log(data);
                    let ip = (JSON.parse(data["Payload"])["ip"]).trim();
                    //console.log(ip + ": " + numProcessed);
                    //ips.push(ip);
                    ips[ip] = [name, region];
                }
    
                if(numProcessed === this.lambda_names.length) {
                    console.log("All IPs have been processed");
                    this.ips = ips;
                    
                    setTimeout(() => {
                        console.log("Collecting ips");
                        this.collectIPs();
                    }, 3*60*1000);
                }
            });
        }
    }
}


function getLambdaNames() {
    let ln = [];
    for(let i = 0; i < 80; i++) {
        ln.push(["hydrate-" + i, "us-east-1"]);
    }
    return ln;
}

let earl = new Earl(getLambdaNames(), "/media/luke/277eaea3-2185-4341-a594-d0fe5146d917/twitter_urls/2019.tsv", "res.tsv", "resdb", ["http://127.0.0.1:8081"], 50);