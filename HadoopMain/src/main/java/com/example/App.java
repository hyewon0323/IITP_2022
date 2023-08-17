package com.example;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Random;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class App { 

    JSONParser parser = new JSONParser();
    try{
        JSONObject jsonObject = (JSONObject) parser.parse(new InputStreamReader(new FileInputStream("Benchmark.json"), "utf-8"));
        String columnNumberList = Object.keys(jsonObject);
        columnNumberList.remove(0);  
        String algorithms [];   
        String options [];   
        
        for(String colNum : columnNumberList){
            //int i = Integer.parseInt(colNum);   
            algorithms.add(jsonObject.get(colNum)[0]);
            options.add(jsonObject.get(colNum));
            //option.add(jsonObject.get(colNum))
        }  
        options.remove(0);
        algorithms.remove(0);

    } catch (ParseException e1) {
        e1.printStackTrace();
    }

    public static class MinMaxTuple implements Writable{ 

        private Double min = Double.MAX_VALUE ;
        private Double max = - Double.MAX_VALUE;

        public Double getMin() {
            return min;
            }
        public void setMin(Double min) {
            this.min = min;
            }

        public Double getMax() {
            return max;
            }   
        public void setMax(Double max) {
            this.max = max;
            }

        public void readFields(DataInput in) throws IOException {
            min = in.readDouble();
            max = in.readDouble();
        }

        public void write(DataOutput out) throws IOException {
            out.writeDouble(min);
            out.writeDouble(max);
        }

        public String toString() {
            return min + "\t" + max + "\t";
            }
    }

    public static class PreTrainMapper extends Mapper<Object, Text, Text, DoubleWritable>{	
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            String algorithm;

            for(int i; i < columnNumberList.length; i++){
                int colNum = Integer.parseInt(columnNumberList[i]);
                algorithm = algorithms[i];
                if(algorithm == "Microaggregation"){
                    columnNumberList = (options[i][1]).split(","); // ["2","3","13"]
                    columnValueList = (options[i][2]).split(","); // ["A","스타저축은행","교직원단체상해보험"]
                    
                    int score = 0;
                    for(int i = 0; i < columnNumberList.length; i++){
                        int groupingCol = Integer.parseInt(columnNumberList[i]);
                        if(values[groupingCol].equals(columnValueList[i])){
                            score++;
                        }
                    }
                    if (score == columnNumberList.length){
                        double outputVal = Double.parseDouble(values[colNum]);
                        context.write(new Text("M" + columnNumberList[i]), new DoubleWritable(outputVal));
                    }
                }
                else if(algorithm == "Topdown"){
                    double outputVal =  Double.parseDouble(values[colNum]);
                    context.write(new Text("T" + columnNumberList[i]), new DoubleWritable(outputVal));
                }
                else if(algorithm == "Randomize"){
                    double outputVal =  Double.parseDouble(values[colNum]);
                    context.write(new Text("R" + columnNumberList[i]), new DoubleWritable(outputVal));
                }
            }
        }     
    }

    public static class PreTrainReducer extends Reducer<Text, Text, IntWritable, Text>{

        private final static MinMaxTuple result = new MinMaxTuple();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0;
            double sumOfSquares = 0;
            String method;
            int column;
            method = key.charAt(0);
            column = Integer.parseInt(key.substring(1, -1));

            if(method == "M" || method == "T"){
                for (DoubleWritable value : values) {
                    count += 1;
                    sum += value.get();
                    sumOfSquares += (value.get() * value.get());
                }
                mean = sum / count;
                double sd = Math.sqrt((sumOfSquares / (count)) - mean*mean);
    
                column = Integer.parseInt(key.deleteCharAt(0));
                context.write(new IntWritable(column), new Text(Double.toString(mean) + "," + Double.toString(3*sd)));     
      
            }
            else if(method == "R"){
                result.setMin(Double.MAX_VALUE);
                result.setMax(-Double.MAX_VALUE);

                for (DoubleWritable value : values) {
                    if (result.getMax() != null && value.get() > result.getMax()) {
                        result.setMax(value.get());
                    }
                    if (result.getMin() != null && value.get() < result.getMin()) {
                        result.setMin(value.get());
                    }
                }
                result.setMin(result.getMin());
                result.setMax(result.getMax());
                context.write(new Text(key.toString()), result);



            }
        }
    }

    public static class HadoopMapper extends Mapper<Object, Text, IntWritable, DoubleWritable>{

        static HashMap<Integer, ArrayList<Double>> Map = new HashMap<Integer, ArrayList<Double>>();
        ArrayList<String> list = new ArrayList<String>();
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException { 
            final File[] folder = new File("tmp").listFiles();
            for (final File fileEntry : folder) {
                if(!fileEntry.isDirectory()) {
                    String filename = "tmp/" + fileEntry.getName();
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "utf-8"));
                    String line = null;
                    while( (line = bufferedReader.readLine()) != null)  {
                        String[] values = line.split("\\s+|,"); 
                        list.add(Double.parseDouble(values[1])); 
                        list.add(Double.parseDouble(values[2]));
                        Map.put(Integer.valueOf(values[0]), list);
                    }
                    bufferedReader.close();  
                }
            }
   
        }

        public String Randomize(String str, String _option){
            String str_out = "";
            
            Random rand = new Random();
            for(int i = 0; i < inputSize; i++) {
                int randomIndex = rand.nextInt(inputString.length()); // inputString의 길이 미만만큼의 랜덤 숫자 생성
                str_out += inputString.charAt(randomIndex); // return할 string에 문자 하나씩 이어붙임
            }
            return str_out;
        }

        public String Masking(String str, String _option, String separator, String mark){
            String[] data = str.split(separator);
	    
            String[] _inputList = _input.split(":"); //_inputList : 
            
            String[] idxList = new String[_inputList.length];
        
            int flag = 0;
            for (int i = 0; i < _inputList.length; i++) {
                //System.out.println(_inputList[i]);
                if ( _inputList[i].equals("0") || _inputList[i].equals("1")) {
                    if (flag == 0) {
                        idxList[i] = Integer.toString(i);
                    }
                        
                    else {
                        idxList[i] = Integer.toString(i-_inputList.length+str.length());
                    }
                }
                else {
                    idxList[i] = Integer.toString(i);
                    flag = 1;
                }     
            }
            int idx = 0;
            String result = "*";
            int flag2 = 0;
            for (int i = 0; i < str.length(); i++) {
                try {
                    if (Integer.toString(i).equals(idxList[idx])) {
                        if (_inputList[idx].equals("1") || _inputList[idx].equals("1-")) {
                            result += str.substring(i, i+1);
                            flag2 = 1;
                        }
                        else {
                            result += mark;
                            flag2 = 0;
                        }
                        idx++;
                    }
                    else {
                        if (flag2==1) {
                            result += str.substring(i, i+1);
                        }
                        else { result += mark; }
                        
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                    //마지막이 0- or 1- 인 경우
                    if (flag2==1) { result += str.substring(i, i+1); }
                    else { result += mark; }
                }
            }
            return result;
        }
        public String PartDelete(String str, String _option, String separator, String mark){
            String[] data = str.split(separator);
	    
            String[] _inputList = _input.split(":"); //_inputList : 
            
            String[] idxList = new String[_inputList.length];
        
            int flag = 0;
            for (int i = 0; i < _inputList.length; i++) {
                //System.out.println(_inputList[i]);
                if ( _inputList[i].equals("0") || _inputList[i].equals("1")) {
                    if (flag == 0) {
                        idxList[i] = Integer.toString(i);
                    }
                        
                    else {
                        idxList[i] = Integer.toString(i-_inputList.length+str.length());
                    }
                    }
                else {
                    idxList[i] = Integer.toString(i);
                    flag = 1;
                }     
            }
            int idx = 0;
            String result = "";
            int flag2 = 0;
            for (int i = 0; i < str.length(); i++) {
                try {
                    if (Integer.toString(i).equals(idxList[idx])) {
                        if (_inputList[idx].equals("1") || _inputList[idx].equals("1-")) {
                            result += str.substring(i, i+1);
                            flag2 = 1;
                        }
                        else {
                            result += mark;
                            flag2 = 0;
                        }
                        idx++;
                    }
                    else {
                        if (flag2==1) {
                            result += str.substring(i, i+1);
                        }
                        else { result += mark; }
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                    //마지막이 0- or 1- 인 경우
                    if (flag2==1) { result += str.substring(i, i+1); }
                    else { result += mark; }
                }
            }
            return result;

        }
        public String Round(String str, String _option, int i){}
        public String Encryption(String str){

            String alg = "AES/CBC/PKCS5Padding";
            String key = "01234567890123456789012345678901";
            String iv = key.substring(0, 16); // 16byte
    
            Cipher cipher = Cipher.getInstance(alg);
            SecretKeySpec keySpec = new SecretKeySpec(key.getBytes(), "AES");
            IvParameterSpec ivParamSpec = new IvParameterSpec(iv.getBytes());
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivParamSpec);
    
            byte[] encrypted = cipher.doFinal(text.getBytes("UTF-8"));
            return Base64.getEncoder().encodeToString(encrypted);
        }
        
        public String Topbottom(String str, int lower, int upper){
            Double lower = mean - sd3;
            Double upper = mean + sd3;
            if(Double.parseDouble(str)< lower || Double.parseDouble(str) > upper){
                return Double.toString(mean);
            }
            else{
                return str;
            }
        }
        
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, DoubleWritable>.Context context) 
            throws IOException, InterruptedException {
            int count = 0;
            String[] values = value.toString().split(",");
            String result = "";
            try{
                for(int i; i < columnNumberList.length; i++){
                    int colNum = Integer.parseInt(columnNumberList[i]);
                    String algorithm = algorithms[i];
                    if(algorithm == "Masking"){
                        String option = options[i][1];
                        values[colNum] = Masking(values[colNum], option, "", "*");
                    }
                    else if(algorithm == "Part-delete"){
                        String option = options[i][1];
                        values[colNum] = PartDelete(values[colNum], option, "", "");
                    }
                    else if(algorithm == "Delete"){
                        values[colNum] = "";
                    }
                    else if(algorithm == "Round"){
                        String option = options[i][1];
                        int option2 = Integer.parseInt(option[i][2]);
                        values[colNum] = Round(values[colNum], option, option2);
                    }
                    else if(algorithm == "Encryption"){
                        values[colNum] = Encryption(values[colNum]);
                    }
                    else if(algorithm == "Aggregation" || algorithm == "Topbottom"){
                        Double mean = Map.get(i)[0];
                        Double sd3 = Map.get(i)[1];
                        values[colNum] = Topbottom(values[colNum], mean, sd3);
                    }
                    
                    else if(algorithm == "Random"){
                        if (MinMap.containsKey(i) == true) {
                            Double min = Map.get(i)[0];
                            Double max = Map.get(i)[1];
                            String randomNum = String.valueOf((Double) (min + (Double)(Math.random() * ((max - min) + 1))));
                            outputData += randomNum;
                        
                        } else {
                            int inputSize = data[i].length();
                            outputData += randomize(inputSize, hashString);
                        
                        }
                        values[colNum] = Randomize(values[colNum], option);
                    }
                    result += values[colNum] + ",";

                }
                context.write(NullWritable.get(), new Text(result));
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    

    public static void main(String[] args) throws Exception { 

        Configuration conf = new Configuration();

        conf.set("mapreduce.job.jvm.numtasks", "-1"); 
        // conf.set("yarn.app.mapreduce.am.resource.mb", "3904");
        // conf.set("mapreduce.map.cpu.vcores", "1"); 
        // conf.set("mapreduce.map.memory.mb", "3904");  
        // conf.set("mapreduce.tasktracker.map.tasks.maximum", "3"); 
        // conf.set("mapreduce.reduce.cpu.vcores", "1"); 
        // conf.set("mapreduce.reduce.memory.mb", "11712");
        // conf.set("mapreduce.tasktracker.reduce.tasks.maximum", "1"); 
        // conf.set("mapreduce.input.fileinputformat.split.maxsize", "317519059"); // 파일 크기 나누기 코어수-1, 232847400/1164236600/6551360000 , 파일크기/70
        // conf.set("mapreduce.input.fileinputformat.split.minsize", "317519059"); // 7(매퍼 수)과 10(파일 수)의 최소공배수:70
        JSONParser parser = new JSONParser();

        try {
            JSONObject jsonObject = (JSONObject) parser.parse(new InputStreamReader(new FileInputStream("Benchmark.json"), "utf-8"));
            String columnNumberList[] = Object.keys(jsonObject);
            String algorithm [];
            //String option [];
            
            for(String colNum : columnNumberList){
                //int i = Integer.parseInt(colNum)
                algorithm.add(jsonObject.get(colNum));
                //option.add(jsonObject.get(colNum))
            }  
            
        } catch (ParseException e1) {
            e1.printStackTrace();
        }

        if(algorithm.contains("Radomize") || algorithm.contains("Microaggregation") || algorithm.contains("Topdown")){
            Job job1 = Job.getInstance(conf, "JOB_1");
            job1.addCacheFile(new URI(args[2]+"#Benchmark.json"));
            job1.setJarByClass(App.class);
            job1.setMapperClass(PretrainMapper.class);
            job1.setReducerClass(PretrainReducer.class);
            job1.setMapOutputKeyClass(IntWritable.class);
            job1.setMapOutputValueClass(DoubleWritable.class);
            job1.setOutputKeyClass(IntWritable.class);
            job1.setOutputValueClass(Text.class);
            
            Path tmp_output = new Path("tmp");
            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(tmp_output.toString()));

            FileSystem hdfs = FileSystem.get(conf);

            if (job1.waitForCompletion(true)) {
                Job job2 = Job.getInstance(conf, "JOB_2");    
                
                FileStatus[] fileStatus = hdfs.listStatus(tmp_output);
                for (FileStatus file : fileStatus) {  
                    String[] filenames = file.getPath().toString().split("/");
                    String filename = "tmp/" + filenames[filenames.length - 1];   
                    job2.addCacheFile(new URI(filename+"#"+filename));          
                }
                
                job2.addCacheFile(new URI("tmp#tmp"));
                job2.addCacheFile(new URI(args[2]+"#Benchmark.json"));
                job2.setJarByClass(App.class);
                job2.setMapperClass(HadoopMapper.class);
                job2.setReducerClass(HadoopReducer.class);
                job2.setMapOutputKeyClass(IntWritable.class);
                job2.setMapOutputValueClass(Text.class);
                job2.setOutputKeyClass(NullWritable.class);
                job2.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job2, new Path(args[0]));
                FileOutputFormat.setOutputPath(job2, new Path(args[1]));
                FileInputFormat.setInputDirRecursive(job2, true);
    
                System.exit(job2.waitForCompletion(true) ? 0 : 1);
            }
            else{
                Configuration conf2 = new Configuration();
                FileSystem hdfs = FileSystem.get(conf2);
                // conf2.set("mapreduce.job.jvm.numtasks", "-1"); 
                // conf2.set("yarn.app.mapreduce.am.resource.mb", "5856"); // 5856나누기 2
                // conf2.set("mapreduce.map.cpu.vcores", "1"); 
                // conf2.set("mapreduce.map.memory.mb", "5856");  
                // conf2.set("mapreduce.tasktracker.map.tasks.maximum", "3"); 
                // conf2.set("mapreduce.input.fileinputformat.split.maxsize", "498958520"); // 파일 크기 나누기 코어수-1, 232847400/1164236600/6551360000 , 파일크기/30
                // conf2.set("mapreduce.input.fileinputformat.split.minsize", "498959000"); // 15(매퍼 수)과 10(파일 수)의 최소공배수:30

                Job job2 = Job.getInstance(conf2, "JOB_2");
                job2.addCacheFile(new URI(args[2]+"#Benchmark.json"));
                job2.setJarByClass(App.class);

                job2.setMapperClass(HadoopMapper.class);
                //job2.setReducerClass(HadoopReducer.class);
                //job2.setNumReduceTasks(0);
                job2.setMapOutputKeyClass(Text.class);
                job2.setMapOutputValueClass(Text.class);
                job2.setOutputKeyClass(NullWritable.class);
                job2.setOutputValueClass(Text.class);

                Path outputPath = new Path(args[1]);
                FileInputFormat.addInputPath(job2, new Path(args[0]));
                FileOutputFormat.setOutputPath(job2, outputPath);
                //FileInputFormat.setInputDirRecursive(job2, true); 
                System.exit(job2.waitForCompletion(true) ? 0 : 1);
            }
        }
    }

}