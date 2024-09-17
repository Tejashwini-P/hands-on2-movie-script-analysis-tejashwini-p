package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text characterWord = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    // Split each line by the first occurrence of ":"
    String line = value.toString();
    String[] parts = line.split(":", 2);
    
    if (parts.length == 2) {
        String characterName = parts[0].trim(); // Character's name
        String dialogue; 
        dialogue = parts[1].trim(); // The dialogue

        // Tokenize the dialogue into words
        StringTokenizer tokenizer = new StringTokenizer(dialogue);

        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken().toLowerCase().replaceAll("[^a-zA-Z]", ""); // Clean the word
            if (!word.isEmpty()) {
                characterWord.set(characterName + " " + word); // Set key as "CharacterName Word"
                context.write(characterWord, one); // Emit key-value pair
            }
        }
    }
}
}
    

