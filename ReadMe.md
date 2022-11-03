Thanks for focus on this project.

The test tool is designed to test the performance of different File System(HDFS/Alluxio/S3) through different 
configuration attributes.

## How to start

* Generate a tar file by `mvn clean package` firstly.
* Transform the package to where you need
* Unzip the package by `tar -xzf Compatibility-TestTool-1.1-alpha-snapshot.tar.gz`
* Change the configurations in `config.properties`
* run the tool by `java -jar `


## Configuration
> ##-----------common----------##
> #file system host address  
> host=alluxio://localhost:19998  
> #host=s3a://testbucket/  
> 
> #op name, now support check/create/delete/read/rest-read/loop  
> op_name=create  
> 
> #task directory path  
> work_path=/test_benchmark_fs/
> 
> #other config file path(core-site.xml,hdfs-site.xml,alluxio-site.properties)
config=.
>
> #total threads  
> total_threads=10
>
> #username that will access the file system  
> user_name=alluxio
> 
>##-----------check-----------##
>
>##-----------create----------##
>#total file number  
>total_files=100  
>#the file name type can be random/inOrder  
>#random - the file name will be random string  
>#inOrder - the file name will be 'prefix'-0, 'prefix'-1, 'prefix'-2...  
>create_file_name_type=random  
> 
>#the file size for uploading  
>create_size_per_file=100  
> 
>#the local file templet path.  
>create_file_path=./testfile  
> 
>#the prefix of the name will be created  
>create_file_prefix=test
>
>##-----------read------------##  
>read_duration_time=360
>
>##-----------rest-read-------##  
>web_host=http://localhost:39999/api/v1/s3
>
>##-----------delete----------##  
>#the directory to be emptied  
>delete_file_prefix=/test_benchmark_fs/
>
>##-----------MIX-------------##  
>#ops=create,read,delete  
>ops=create,write  
>loopCount=300
>

### Write File Task
if you're gonna create a write task in the tool, you need this following configurations:

### Read File Task

### Rest-Read Task

### Delete Task

### Loop Task