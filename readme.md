 - c.js: Coordinating server.  Responsible for distributing requests to the request servers.
 - s.js: Request server.  Responsible for pushing requests from the coordinating server to the Lambda instances.
 - l.js: Code to launch and update the Lambda instances themselves.
 - aws_main.py: The code that runs on each of the Lambda instances