var AWS = require('aws-sdk');

// Set the region 
AWS.config.update({region: 'us-east-1'});
var fs = require('fs');

/* Create/update the lambdas. */
function launch(count, numLaunched, region, update) {
    if(numLaunched < count) {
        AWS.config.update({region: region});
        let lambda = new AWS.Lambda();

        var params = {
            Code: {
                ZipFile: fs.readFileSync("lambda.zip")
            }, 
            Description: "Accepts a URL and rehydrates it", 
            FunctionName: "hydrate-" + numLaunched, 
            Handler: "main_aws.lambda_handler",
            MemorySize: 128, 
            Publish: true, 
            Role: "arn:aws:iam::252108313661:role/service-role/expand1-role-9minhczh",
            Runtime: "python3.7", 
            Timeout: 5, 
            VpcConfig: {
            }
        };
        
        /*let f = lambda.createFunction;
        if(update) {
            f = lambda.updateFunctionConfiguration;
        }*/

        lambda.createFunction(params, (err, data) => {
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

/* Update the lambdas */
function updateConfig(count, numUpdated, region) {
    if(numUpdated < count) {
        AWS.config.update({region: region});
        let lambda = new AWS.Lambda();

        var params = {
            FunctionName: "hydrate-" + numUpdated, 
            Timeout: 60 
        };
        
        lambda.updateFunctionConfiguration(params, (err, data) => {
            if(err) {
                console.log(err);
                setTimeout(() => { updateConfig(count, numUpdated, region); }, 1000);
            }
            else {
                console.log("Updated " + numUpdated);
                setTimeout(() => { updateConfig(count, numUpdated + 1, region) }, 1000);
            }
        });
    }
}

/* Update the lambdas */
function updateCode(count, numUpdated, region) {
    if(numUpdated < count) {
        AWS.config.update({region: region});
        let lambda = new AWS.Lambda();

        var params = {
            ZipFile: fs.readFileSync("lambda.zip"),
            FunctionName: "hydrate-" + numUpdated, 
        };
        
        lambda.updateFunctionCode(params, (err, data) => {
            if(err) {
                console.log(err);
                setTimeout(() => { updateCode(count, numUpdated, region); }, 1000);
            }
            else {
                console.log("Updated code for " + numUpdated);
                setTimeout(() => { updateCode(count, numUpdated + 1, region) }, 1000);
            }
        });
    }
}

//launch(900, 0, "us-east-1");
updateConfig(900, 0, "us-east-1");
updateCode(900, 0, "us-east-1");