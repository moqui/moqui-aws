# Moqui AWS Integrations

Amazon Web Services tool integrations for Moqui Framework

* In order to work with AWS, you must have an AWS account.  
For security purposes create IAM user with administrator access instead of using just your root user.  
More information about creating a user can be found [here](https://docs.aws.amazon.com/mediapackage/latest/ug/setting-up-create-iam-user.html)

* Create an S3 bucket with policies that allow you to upload data.

* To add moqui-aws component to Moqui simply clone this repository in your-moqui-project-name/runtime/component folder  
or use the command ./gradlew getComponent -Pcomponent=moqui-aws.

* Add your credentials at moqui-aws/MoquiConf.xml file.  
You can check your region code [here](https://docs.aws.amazon.com/general/latest/gr/rande.html).

* Run your moqui app. To be able to save in the AWS S3 bucket go to System -> User groups and choose ALL_USERS group.  
At the Preferences section for the keys **mantle.content.large.root**  and **mantle.content.root** set the values "aws3://your-bucket-name".

* After applying these settings your S3 bucket will be a data storage house for your application.
