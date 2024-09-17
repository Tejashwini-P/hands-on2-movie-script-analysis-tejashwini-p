package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class UniqueWordsMapper extends Mapper<Object, Text, Text, Text> {

    private Text character = new Text();
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Split each line by the first occurrence of ":"
      String line = value.toString();
      String[] parts = line.split(":", 2);
      
      if (parts.length == 2) {
          String characterName = parts[0].trim(); // Character's name
          String dialogue = parts[1].trim(); // The dialogue

          // Tokenize the dialogue into words
          StringTokenizer tokenizer = new StringTokenizer(dialogue);

          while (tokenizer.hasMoreTokens()) {
              String wordStr = tokenizer.nextToken().toLowerCase().replaceAll("[^a-zA-Z]", ""); // Clean the word
              if (!wordStr.isEmpty()) {
                  character.set(characterName); // Set character name
                  word.set(wordStr); // Set the word
                  context.write(character, word); // Emit key-value pair (character, word)
              }
          }
      }
  }
}
    
