package dsp2015;

import java.io.*;
import java.util.UUID;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

/**
 * Created by ran on 15/08/15.
 */
public class dsp3EmrClient {
    public static String propertiesFilePath = "/home/barashe/dsp/cred.properties";

    public static void main(String[] args) throws FileNotFoundException, IOException{

        AWSCredentials credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
        mapReduce.setRegion(Region.getRegion(Regions.US_EAST_1));
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://ranerandsp4/DSP3-1.0-SNAPSHOT.jar") // This should be a full map reduce application.
                .withMainClass("dsp2015.Flow")
                .withArgs("20","s3n://ranerandsp4/output-"+UUID.randomUUID()+"/","0","0");

        StepConfig stepConfig = new StepConfig()
                                .withName("flow")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withEc2KeyName("raneran")
                .withInstanceCount(5)
                .withMasterInstanceType(InstanceType.M1Large.toString())
                .withSlaveInstanceType(InstanceType.C3Xlarge.toString())
                .withHadoopVersion("2.6.0").withEc2KeyName("raneran")
                .withKeepJobFlowAliveWhenNoSteps(false);
        //.withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest().withReleaseLabel("emr-4.0.0")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withName("DSP3flow")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri("s3n://ranerandsp4/logs/");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
}
}
