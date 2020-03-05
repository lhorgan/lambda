var AWS = require('aws-sdk');
var AWS = require('aws-sdk');
// Set the region 
AWS.config.update({region: 'us-east-1'});
var fs = require('fs');
const lineByLine = require('n-readlines');
var level = require('level');

var lambda = new AWS.Lambda();

class Earl {
    constructor(lambda_names, ifname, ofname, dbname) {
        this.lambda_names = lambda_names;
        this.ips = {};
        this.readstream = new lineByLine(ifname);
        this.db = level(dbname);
        this.allLinesRead = false;
        this.urlsToWrite = [];
        this.ofname = ofname;
        this.numProcessed = 0;

        this.collectIPs(() => {
            setTimeout(() => {
                console.log("Collecting ips");
                this.collectIPs();
            }, 3*60*1000);
        });

        setInterval(async () => {
            await this.expand();
        }, 1000);
    }

    async expand() {
        let ipFreeze = JSON.parse(JSON.stringify(this.ips));

        for(let ip in ipFreeze) {
            let [url, year] = await this.getNextURL();
            let name = ipFreeze[ip][0];
            let region = ipFreeze[ip][1];

            AWS.config.update({region: region});

            var params = {
                FunctionName: name,
                Payload: JSON.stringify({"url": url})
            };

            lambda.invoke(params, (err, data) => {
                let message = {};
                if(err) {
                    //console.log("There was an error");
                    message["orig_url"] = url;
                    message["err"] = true;
                    message["msg"] = err.toString();
                    message["url"] = "";
                    message["time"] = "";
                }
                else {
                    //console.log("we successed");
                    console.log(data);
                    
                    let res = JSON.parse(data["Payload"]);

                    if(res.errorMessage) {
                        message["orig_url"] = url;
                        message["err"] = true;
                        message["msg"] = res.errorMessage;
                        message["time"] = "";
                        message["url"] = "";
                    }
                    else {
                        message["err"] = res.error;
                        message["orig_url"] = res.orig_url;

                        if(!res.orig_url) {
                            console.log("\n\n\nTHIS ONE IS DEAD\n\n\n");
                        }

                        message["url"] = res.url;
                        message["time"] = res.diff;
                        message["msg"] = res.message;
                    }
                }
                this.numProcessed++;
                console.log(message["orig_url"] + " --< " + message["url"] + ": " + this.numProcessed);
                this.urlsToWrite.push(message);
                if(this.urlsToWrite.length >= 50) {
                    this.writeURLs();
                }
            });
        }
    }

    writeURLs() {
        console.log("Writing a batch of URLs");
        let urlsCopy = JSON.parse(JSON.stringify(this.urlsToWrite)); // live with it
        this.urlsToWrite = [];
        let urlStr = "";
        for(let i = 0; i < urlsCopy.length; i++) {
            urlStr += urlsCopy[i].url + "\t" + 
                      urlsCopy[i].orig_url + "\t" + 
                      urlsCopy[i].time + "\t" +
                      urlsCopy[i].err + "\t" +
                      urlsCopy[i].msg;
            urlStr += "\r\n";

            console.log(urlsCopy[i]);
            console.log("ORIG URL: " + urlsCopy[i].orig_url);
            this.db.put(urlsCopy[i].orig_url, "", () => {});
        }

        var stream = fs.createWriteStream(this.ofname, {flags:'a'});
        stream.write(urlStr);
        stream.end();
    }

    async getNextURL() {
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

    collectIPs(cb) {
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
                    cb();
                }
            });
        }
    }
}

/* This example creates a Lambda function. */
function launch(count, numLaunched, region, update) {
    if(numLaunched < count) {
        AWS.config.update({region: region});

        var params = {
            Code: {
                ZipFile: fs.readFileSync("lambda.zip")
            }, 
            Description: "Accepts a URL and rehydrates it", 
            FunctionName: "hydrate-" + numLaunched, 
            Handler: "main_aws.lambda_handler", // is of the form of the name of your source file and then name of your function handler
            MemorySize: 128, 
            Publish: true, 
            Role: "arn:aws:iam::252108313661:role/service-role/expand1-role-9minhczh",
            Runtime: "python3.7", 
            Timeout: 5, 
            VpcConfig: {
            }
        };
        
        let f = lambda.createFunction;
        if(update) {
            f = lambda.updateFunctionConfiguration;
        }

        f(params, (err, data) => {
            if(err) {
                console.log(err);
                setTimeout(() => { launch(count, numLaunched, region); }, 1000);
            }
            else {
                console.log("Launched " + numLaunched);
                setTimeout(() => { launch(count, numLaunched + 1, region) }, 1000);
            }
        });
    }
}

/* This example creates a Lambda function. */
function update(count, numUpdated, region) {
    if(numUpdated < count) {
        AWS.config.update({region: region});

        var params = {
            ZipFile: fs.readFileSync("lambda.zip"),
            Publish: true,
            FunctionName: "hydrate-" + numUpdated, 
        };
        
        lambda.updateFunctionCode(params, (err, data) => {
            if(err) {
                console.log(err);
                setTimeout(() => { update(count, numUpdated, region); }, 1000);
            }
            else {
                console.log("Updated " + numUpdated);
                setTimeout(() => { update(count, numUpdated + 1, region) }, 1000);
            }
        });
    }
}

//update(900, 0, "us-east-1");
//launch(900, 501, "us-east-1");
//collectIPs("us-east-1", 900, 0);
function getLambdaNames() {
    let ln = [];
    for(let i = 0; i < 70; i++) {
        ln.push(["hydrate-" + i, "us-east-1"]);
    }
    return ln;
}

let earl = new Earl(getLambdaNames(), "/media/luke/277eaea3-2185-4341-a594-d0fe5146d917/twitter_urls/2019.tsv", "res.tsv", "resdb");



// AWS.config.update({region: "us-east-1"});

// var params = {
//     FunctionName: "hydrate-1",
//     Payload: JSON.stringify({"url": "https://dy.si/VTXLju"})
// };

// lambda.invoke(params, (err, data) => {
//     console.log(err);
//     console.log(data);
//});