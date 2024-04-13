#!/usr/bin/env python3
import os

import aws_cdk as cdk

from cdk.cdk_stack import CdkStack
from cdk.cdk_stack_web_hosting import CdkStackWebHosting


app = cdk.App()

# main application stack which can be deployed to any region that contains the required services and Amazon Bedrock models.
CdkStack(app, 
    "ConnectionsInsights-main",
    env=cdk.Environment(region=os.environ["CDK_DEPLOY_REGION"])
)

# web application stack that can only be deployed to us-east-1 as it requires AWS WAF.
CdkStackWebHosting(app, 
    "ConnectionsInsights-webapp",
    env=cdk.Environment(region="us-east-1")
)
app.synth()