Thanks for focus on this project.

The test tool is designed to test the performance of different File System(HDFS/Alluxio/S3) through different 
configuration attributes.

## How to start

You can easily get the tar package with `mvn package` and then you can find the “Compatibility-TestTool-xxxx-snapshot.tar.gz” 
under “/target” directory. Then you can move this package to the client env node and decompress it.
After decompression, you can see these files and directory in the decompressed directory.
> /common-logging.properties  
> /Compatibility-TestTool-1.1-alpha.jar  
> /config.properties  
> /core-site.xml  
> /lib/  
> /log4j.properties  


* Generate a tar file by `mvn clean package` firstly.
* Transform the package to where you need
* Unzip the package by `tar -xzf Compatibility-TestTool-1.1-alpha-snapshot.tar.gz`
* Change the configurations in `config.properties`
* run the tool by `java -jar xxx.jar`


## Configuration

### Configuration when you want to test Alluxio API
If you want to test the performance of Alluxio filesystem api, you can follow this steps and configuration to get it. 
You need to update the configurations in the “config.properties”:
```
##-----------common----------##
host=alluxio://localhost:19998

# op name, now support check/create/delete/read/loop
op_name=read
work_path=/test_benchmark_fs
total_threads=10
user_name=alluxio

##-----------create----------##
total_files=100
create_file_name_type=random
create_size_per_file=100

##-----------read------------##
read_duration_time=200
read_buffer_size=1048576

##-----------delete----------##
delete_file_prefix=/test_benchmark_fs/

##-----------MIX-------------##
mix_read_percentage=75
mix_create_percentage=25
```
You need to update these configurations in the “core-site.xml”:
```
<configuration>
    <property>
        <name>fs.file.impl</name>
        <value>org.apache.hadoop.fs.LocalFileSystem</value>
    </property>
    <property>
        <name>fs.alluxio.impl</name>
        <value>alluxio.hadoop.FileSystem</value>
    </property>
</configuration>
```

### Configuration when you want to test S3 API
There are two ways to access S3 api of Alluxio. They both work well in the tool, you can choose one of them to test 
the performance(notice: The two different way are different in some implementation, so they has different performance)
- S3A Connector

You can leverage S3A connector to access Alluxio. If you want to try this way, you need to change these configurations 
in the “config.properties”:
```
##-----------common----------##
host=s3a://testbucket/
# in this situation, op can be check/create/read/delete/loop
op_name=read
work_path=/test_benchmark_fs
total_threads=10
user_name=alluxio

##-----------create----------##
total_files=100
create_size_per_file=100

##-----------read------------##
read_duration_time=200
read_buffer_size=1048576

```

And you need to update these configurations in the “core-site.xml”:
```
<configuration>
    <property>
        <name>fs.s3a.access.key</name>
        <value>alluxio</value>
    </property>
    <property>
        <name>fs.s3a.secret.key</name>
        <value>[sk]</value>
    </property>
    <property>
        <name>fs.s3a.connection.ssl.enabled</name>
        <value>false</value>
    </property>
    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.s3a.endpoint</name>
        <value>http://localhost:39999/api/v1/s3</value>
    </property>
    <property>
        <name>fs.s3a.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>
    <property>
        <name>fs.s3a.aws.credentials.provider</name>
        <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
    </property>
</configuration>
```

- HTTP Request

You can send http requests to Alluxio S3 Rest Server. If you want to try this way, you need to change these 
configurations in the “config.properties”:
```
##-----------common----------##
# in this case, op can be rest-create/rest-read/loop
op_name=rest-read
work_path=/test_benchmark_fs
total_threads=10
user_name=alluxio

##-----------create----------##
# bucket name in rest-create task
bucket_name=testbucket
total_files=100
create_file_name_type=random
create_size_per_file=100

##-----------read------------##
read_duration_time=200
read_buffer_size=1048576

##-----------rest-read-------##
web_host=http://localhost:39999/api/v1/s3
```

## Check test result

After you run the command “java -jar java -jar Compatibility-TestTool-1.1-alpha.jar”, you can see there will be 
continuous output on the console. Now it will output the throughput every 5 seconds. (It’s just for checking the 
performance in time, maybe I will change it in the future.) The metrics in the output are the iops and latency in 
different tasks.


After the task is completed, you can check the “tool.log” file to get the summary of this test task result. The result 
contains the qps, throughput, total op counts and latency(including min, max, mean, median, p75, p99).

