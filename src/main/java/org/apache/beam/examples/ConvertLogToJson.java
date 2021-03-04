package org.apache.beam.examples;

import org.apache.avro.JsonProperties.Null;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Regex;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import com.sun.tools.javac.parser.ReferenceParser.ParseException;


public class ConvertLogToJson {
  
  public interface ConvertLogToJsonOptions extends PipelineOptions {

    @Description("Path of the file to read from")
    @Default.String("input.txt")
    String getInputFile();

    void setInputFile(String value);

    /** Set this required option to specify where to write the output. */
    @Description("Path of the folders to write to")
    @Required
    String [] getOutput();
    void setOutput(String [] value);
  }

  static void runConvertLog(ConvertLogToJsonOptions options) {
        
    // Create tags to use for the main and additional outputs.
    final TupleTag<String> archiveTag = new TupleTag<String>(){};
    final TupleTag<String> log1Tag =   new TupleTag<String>(){};
    final TupleTag<String> log2Tag =  new TupleTag<String>(){};
    
    Pipeline p = Pipeline.create(options);
        
    PCollection<String> lines=p.apply("ReadLines", TextIO.read().from(options.getInputFile()));  
    String regex_str="(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})\\s(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})\\s(\\d{10}|\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z)\\s(\\d{1,6})\\s(\\d{1,4})\\s(\\d{1,3})\\s(true|false)\\s(\\d)";
    PCollection <List<String>> values =lines.apply(Regex.allMatches(regex_str));

     PCollectionTuple results = values.apply(ParDo.of(new DoFn<List<String>, String>() {                 
            @ProcessElement
             public void processElement(ProcessContext c) {
               List<String> value = c.element();
               
               String id=value.get(8);     
               String timeAsISO=value.get(3);
               //if time is in timestamp  format then convert it to iso86 
               if (timeAsISO.length()<11){
                TimeZone tz = TimeZone.getTimeZone("UTC");
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'"); 
                df.setTimeZone(tz);
                timeAsISO=df.format(new Date(Long.parseLong(value.get(3))*1000));
               }           

               String json_str="{\"destinationIp\":\""+value.get(1)+"\",\"destinationPort\":\""+value.get(5)+"\",\"sourcePort\":\""+value.get(6)+"\",\"sourceIp\":\""+value.get(2)+"\",\"bytes\":\""+value.get(4)+"\",\"authorized\":\""+value.get(7)+"\",\"logId\":\""+value.get(8)+"\",\"timestamp\":\""+timeAsISO+"\"}";
               
               //save to main location
               c.output(archiveTag,json_str);

               //route to separate folders depending on id
               if (id.equals("1")){
                     c.output(log1Tag,json_str);
                }
                else if(id.equals("2")){
                    c.output(log2Tag,json_str);
                }
             }})
             // Specify the main and consumed output tags of the PCollectionTuple
         .withOutputTags(archiveTag, TupleTagList.of(log1Tag).and(log2Tag)));
        
        // Extract the PCollection results, by tag.
        results.get(archiveTag).apply(TextIO.write().to(options.getOutput()[0]+"/archive.txt").withNumShards(1));
        results.get(log1Tag).apply(TextIO.write().to(options.getOutput()[1]+"/output1.txt").withNumShards(1));
        results.get(log2Tag).apply(TextIO.write().to(options.getOutput()[2]+"/output2.txt").withNumShards(1));
    p.run().waitUntilFinish();

  }

  public static void main(String[] args) {
    ConvertLogToJsonOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ConvertLogToJsonOptions.class);
    runConvertLog(options);
  }
}
