var express = require('express');
var app = express();
var bodyParser = require('body-parser')
var AWS = require('aws-sdk');

app.use(bodyParser.json());

class Earl {
    constructor() {
        this.configure();
    }

    configure() {
        app.listen(8081, function () {
            console.log("App listening on port 8081");
        });

        app.post("/urls", (req, res) => {
            console.log("urls received");
            console.log(req.body);
            let urls = req.body;
            this.expand(urls, (messages) => {
                res.send(JSON.stringify(messages));
            });
        });
    }
    
    expand(urls, cb) {
        let messages = [];

        for(let i = 0; i < urls.length; i++) {
            let [name, region, url, options] = urls[i];
            //console.log("Setting region to "  + region);

            AWS.config.update({region: region});
            var lambda = new AWS.Lambda();
            
            let payload = {"url": url};
            if(typeof(options) === "object") {
                for(let option in options) {
                    payload[option] = options[option];
                }
            }

            var params = {
                FunctionName: name,
                Payload: JSON.stringify(payload)
            };

            lambda.invoke(params, (err, data) => {
                let message = {};
                if(err) {
                    //console.log("There was an error");
                    message["orig_url"] = url;
                    message["err"] = true;
                    message["msg"] = err.toString();
                    message["time"] = "";
                    message["blurb"] = {};
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
                        message["blurb"] = {};
                    }
                    else {
                        message["err"] = res.error;
                        message["orig_url"] = res.orig_url;
                        message["url"] = res.url;
                        message["time"] = res.diff;
                        message["msg"] = res.message;
                        message["blurb"] = res.json;
                    }
                }
                messages.push(message);
                
                if(messages.length === Object.keys(urls).length) {
                    cb(messages);
                }
            });
        }
    }
}

let e = new Earl();