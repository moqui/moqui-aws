# Moqui AWS Integrations

Amazon Web Services tool integrations for Moqui Framework

* To be able to work with aws you should have an amazon user. <br>
For secure purposes create IAM user with administrator access instead of using just your root user. <br>
More information about creating a user you can find [here](https://docs.aws.amazon.com/mediapackage/latest/ug/setting-up-create-iam-user.html)

* Create an S3 bucket with policies that allow you to upload data.

* To add moqui-aws component to Moqui simply clone this repository in your-moqui-project-name/runtime/component folder <br>
or use command ./gradlew getComponent -Pcomponent=moqui-aws.

* Add your credentials at moqui-aws/MoquiConf.xml file. <br>
You can check your region code [here](https://docs.aws.amazon.com/general/latest/gr/rande.html).

* Run your moqui app. To be able to save in the AWS S3 bucket go to System -> User groups and choose ALL_USERS group. <br>
At the Preferences section for keys **mantle.content.large.root**  and **mantle.content.root** set values aws3://your-bucket-name.

* After setting these settings you will have an S3 bucket as data storage for your application.
