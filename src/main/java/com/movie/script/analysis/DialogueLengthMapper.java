package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DialogueLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable wordCount = new IntWritable();
    private Text character = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
     // Split each line by the first occurrence of ":"
     String line = value.toString();
     String[] parts = line.split(":", 2);
     
     if (parts.length == 2) {
         String characterName = parts[0].trim(); // Character's name
         String dialogue = parts[1].trim(); // The dialogue

         // Tokenize the dialogue into words and count them
         StringTokenizer tokenizer = new StringTokenizer(dialogue);
         int wordCountValue = tokenizer.countTokens();

         if (wordCountValue > 0) {
             character.set(characterName);  // Set the character name
             wordCount.set(wordCountValue); // Set the word count
             context.write(character, wordCount); // Emit key-value pair (character, wordCount)
         }
     }
 }
}
