import os
from aws_cdk import (
    core,
    aws_iam as iam,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_apigateway as apigw,
    aws_kinesisfirehose as firehose
)


class EwFirehoseS3(core.Stack):

    def __init__(self, scope: core.App, name: str, **kwargs) -> None:
        super().__init__(scope, name, **kwargs)

        bucket = s3.Bucket(
            self, "EwBucket",
            bucket_name="eventwehookbucket",
            removal_policy=core.RemovalPolicy.DESTROY
        )

        """
        firehose_role = iam.Role(
            self, 'FirehoseRole',
            role_name='firehose_role',
            assumed_by=iam.ServicePrincipal('firehose.amazonaws.com'),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess')]
        )
        """

        firehose_role = iam.Role(
            self, 'FirehoseRole',
            role_name='firehose_role',
            assumed_by=iam.ServicePrincipal('firehose.amazonaws.com'),
        )

        firehose_policy_statement = iam.PolicyStatement(
            actions=[
                "s3:AbortMultipartUpload",
                "s3:GetBucketLocation",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:ListBucketMultipartUploads",
                "s3:PutObject"
            ],
            resources=[bucket.bucket_arn, bucket.bucket_arn+'/*']
        )

        firehose_role.add_to_policy(firehose_policy_statement)

        s3_dest_config = firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
            bucket_arn=bucket.bucket_arn,
            role_arn=firehose_role.role_arn
        )

        delivery_stream = firehose.CfnDeliveryStream(
            self, "EwDeliveryStream",
            delivery_stream_name="EW2S3Stream",
            s3_destination_configuration=s3_dest_config
        )

        lambda_func = _lambda.Function(
            self, "EwFirehoseToS3Function",
            code=_lambda.Code.from_asset("lambda"),
            handler="webhook.handler",
            runtime=_lambda.Runtime.PYTHON_3_7,
            environment={
                "FIREHOSE_STREAM": delivery_stream.delivery_stream_name
            }
        )

        lambda_policy_statement = iam.PolicyStatement(
            actions=["firehose:DeleteDeliveryStream",
                     "firehose:PutRecord",
                     "firehose:PutRecordBatch",
                     "firehose:UpdateDestination"
                     ],
            resources=[delivery_stream.attr_arn]
        )

        lambda_func.add_to_role_policy(lambda_policy_statement)

        api = apigw.RestApi(self, "EwToFirehoseApi")
        api.root.add_method("POST", apigw.LambdaIntegration(lambda_func))


app = core.App()
EwFirehoseS3(
    app, "EwFirehoseToS3Stack",
    env={
        "region": os.environ["CDK_DEFAULT_REGION"],
        "account": os.environ["CDK_DEFAULT_ACCOUNT"]
    }
)
app.synth()
