package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class UniqueWordsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // Use a HashSet to collect unique words
      HashSet<String> uniqueWords = new HashSet<>();

      // Add each word to the set
      for (Text value : values) {
          uniqueWords.add(value.toString());
      }

      // Prepare a comma-separated list of unique words
      StringBuilder wordsList = new StringBuilder();
      for (String word : uniqueWords) {
          if (wordsList.length() > 0) {
              wordsList.append(", ");
          }
          wordsList.append(word);
      }

      // Emit the character and the list of unique words
      context.write(key, new Text(wordsList.toString()));
  }
}
    
