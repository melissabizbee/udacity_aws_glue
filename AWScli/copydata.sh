# samples of data i.e. 1 file

aws s3 cp ./Documents/GitHub/udacity_aws_glue/data/customer/customer-keep-1655293787679.json s3://stedi-aws-project/landing/customer

aws s3 cp ./Documents/GitHub/udacity_aws_glue/data/step_trainer/step_trainer-1655296678763.json s3://stedi-aws-project/landing/step-trainer

aws s3 cp ./Documents/GitHub/udacity_aws_glue/data/accelerometer/accelerometer-1655296678763.json s3://stedi-aws-project/landing/accelerometer

# all files in bucket i.e. recursive


aws s3 cp ./Documents/GitHub/udacity_aws_glue/data/accelerometer/ s3://stedi-aws-project/landing/accelerometer --recursive

aws s3 cp ./Documents/GitHub/udacity_aws_glue/data/step_trainer/ s3://stedi-aws-project/landing/step-trainer --recursive
