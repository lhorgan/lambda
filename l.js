var AWS = require('aws-sdk');
var AWS = require('aws-sdk');
// Set the region 
AWS.config.update({region: 'us-east-1'});
var fs = require('fs');

process.env.UV_THREADPOOL_SIZE = 128;

var lambda = new AWS.Lambda();

function collectIPs(region, count, start) {
    AWS.config.update({region: region});
    let ips = [];
    let numProcessed = 0;

    for(let i = 0; i < count; i++) {
        var params = {
            FunctionName: "hydrate-" + (start + i), /* required */
            Payload: JSON.stringify({"ip": true})
        };

        lambda.invoke(params, function(err, data) {
            numProcessed++;
            if(err) {
                console.log("Something went wrong");
            } // an error occurred
            else {
                console.log(data);
                let ip = JSON.parse(data["Payload"])["ip"];
                console.log(ip + ": " + numProcessed);
                ips.push(ip);
            }

            if(numProcessed === count) {
                console.log("All IPs have been processed");
                let uniqueIPs = new Set(ips);
                console.log(uniqueIPs.size);
            }
        });
    }
    console.log("\n\nALL INVOCATIONS HAVE BEEN MADE\n\n");
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

        f(params, function(err, data) {
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
        
        lambda.updateFunctionCode(params, function(err, data) {
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
collectIPs("us-east-1", 150, 0);