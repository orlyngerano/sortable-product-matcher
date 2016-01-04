import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    public static File askValidFileInput(String message, File file){
         if(file==null || !file.exists()){
            System.out.print(message);
            InputStreamReader inputStreamReader =new InputStreamReader(System.in);
            BufferedReader bufferedReader=new BufferedReader(inputStreamReader);
            try {
                file = askValidFileInput(message, new File(bufferedReader.readLine()));
                bufferedReader.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
        return file;
    }


    public static File createFileOrClear(String filePath){
        File file = new File(filePath);
        try {
            if(!file.createNewFile()){
                FileWriter fileWriter= new FileWriter(file);
                fileWriter.write("");
                fileWriter.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }

        return file;
    }


    public static void main(String[] args) {

       final String productInputMssg =  "Please provide correct file path for Product file (ex. /sample_product):";
       final String productListingInputMssg =  "Please provide correct file path for Product listing file  (ex. /sample_listing):";
       final String productOutputMssg =  "Please provide correct file path for Output file (ex. /sample_output) overwrites existing:";

       File productFile=null;
       File productListingFile=null;
       File productOutputFile=null;

       int numArgs = args.length;
       /*if user provide inputs during execution*/
       if(numArgs>=1){
           productFile = askValidFileInput(productInputMssg,new File(args[0]));
       }
       if(numArgs>=2){
           productListingFile = askValidFileInput(productListingInputMssg,new File(args[1]));
       }
       if(numArgs>=3){
           productOutputFile = askValidFileInput(productOutputMssg, createFileOrClear(args[2]));
       }

       /*if user did not provide inputs during execution*/
       if(productFile==null){
           productFile = askValidFileInput(productInputMssg,null);
       }

       if(productListingFile==null){
           productListingFile = askValidFileInput(productListingInputMssg,null);
       }
       if(productOutputFile==null){
           productOutputFile = askValidFileInput(productOutputMssg, createFileOrClear(args[2]));
       }

        System.out.println("program is running. please wait...");
        ProductMatcher productMatcher = new ProductMatcher(productFile, productListingFile, productOutputFile);
        productMatcher.findMatch();
        System.out.println("program end.");
    }
}

class ProductMatcher{

    private File productFile;
    private File productListingFile;
    private File productOutputFile;

    private String cacheOutputItems;
    private static final Object cacheLockObject = new Object();

    public ProductMatcher(File productFile, File productListingFile, File productOutputFile){
        this.productFile = productFile;
        this.productListingFile = productListingFile;
        this.productOutputFile = productOutputFile;
        this.cacheOutputItems = "";
    }

    /*
     * starting point in matching process
     * blocks everything until all worker threads are done
     */
    public void findMatch(){

        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        JsonFactory jsonFactory = new JsonFactory();
        try {
            JsonParser jsonParser = jsonFactory.createParser(productFile);
            JsonToken token=jsonParser.nextToken();

            Product product=null;
            while(jsonParser.hasCurrentToken()){
                if(token==JsonToken.START_OBJECT){
                    product = new Product("","","","");
                }else if(token==JsonToken.END_OBJECT){

                    //let thread pool manage all threads for each product vs listing item match
                    executorService.execute(new ProductListingMatcherThread(product));

                }else{
                    String fieldName = jsonParser.getCurrentName().toUpperCase();
                    jsonParser.nextToken();
                    if(fieldName.equals("PRODUCT_NAME")){
                        product.setProductName(jsonParser.getValueAsString());
                    } else if (fieldName.equals("MANUFACTURER")){
                        product.setManufacturer(jsonParser.getValueAsString());
                    } else if (fieldName.equals("MODEL")){
                        product.setModel(jsonParser.getValueAsString());
                    }else if (fieldName.equals("FAMILY")){
                        product.setFamily(jsonParser.getValueAsString());
                    }
                }
                token = jsonParser.nextToken();
            }

            jsonParser.close();
        }catch (IOException e){
            e.printStackTrace();
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {   }

        flushCacheToOutputFile();


    }

    private void flushCacheToOutputFile(){
        if(!cacheOutputItems.isEmpty()){
            BufferedWriter bufferedWriter = null;
            try {
                bufferedWriter = new BufferedWriter(new FileWriter(productOutputFile,true));
                bufferedWriter.write(cacheOutputItems);
                bufferedWriter.flush();
                bufferedWriter.close();
                cacheOutputItems="";
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }



    /*
    *Thread try to search match listing items of a product
    */
    private class ProductListingMatcherThread implements Runnable{

        private Product product;

        public ProductListingMatcherThread(Product product){
            this.product = product;
        }

        @Override
        public void run() {
            ArrayList<String> productListings = findListingItemsOfProduct(product);

            String productListingMixJson="";
            if(productListings.size()>0){
                StringBuilder productListingBuilder = new StringBuilder();
                for(String productListing : productListings){
                    productListingBuilder.append(productListing).append(",");
                }
                productListingMixJson=productListingBuilder.substring(0, productListingBuilder.length() - 1);
            }


            String productOutput = "{\"product_name\":\""+product.getProductName()+"\",\"listings\":["+productListingMixJson+"]}\n";

            boolean isCacheFull=false;
            synchronized (cacheLockObject){
                if(cacheOutputItems.split("\n").length>9){
                    isCacheFull=true;
                    productOutput=cacheOutputItems+productOutput;
                    //clear cache
                    cacheOutputItems="";
                }else{
                    //cache this time
                    cacheOutputItems+=productOutput;
                }
            }


            /*
            *since expensive to write always, write to file only when cache is full
            */
            if(isCacheFull){
                BufferedWriter bufferedWriter = null;
                try {
                    bufferedWriter = new BufferedWriter(new FileWriter(productOutputFile,true));
                    bufferedWriter.write(productOutput);
                    bufferedWriter.flush();
                    bufferedWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private ArrayList<String> findListingItemsOfProduct(Product product){
            ArrayList<String> productListing = new ArrayList<String>();

            try {
                FileReader fileReader = new FileReader(productListingFile);
                BufferedReader bufferedReader = new BufferedReader(fileReader);

                String productDetailsJson=null;
                while((productDetailsJson = bufferedReader.readLine())!=null){
                    if(Helpers.isListingItemMatchToProduct(productDetailsJson, product)){
                        productListing.add(productDetailsJson);
                    }
                }
                bufferedReader.close();

            }catch (FileNotFoundException e){
                e.printStackTrace();
            }catch (IOException e){
                e.printStackTrace();
            }


            return productListing;
        }
    }

    /*
    *Class representing the Product Item
    */
    private class Product{
        private String productName;
        private String manufacturer;
        private String model;
        private String family;

        public Product(String productName, String manufacturer,String model,String family){
            this.productName=productName;
            this.manufacturer=manufacturer;
            this.model=model;
            this.family=family;
        }

        public String getProductName() {
            return productName;
        }

        public void setProductName(String productName) {
            this.productName = productName;
        }

        public String getManufacturer() {
            return manufacturer;
        }

        public void setManufacturer(String manufacturer) {
            this.manufacturer = manufacturer;
        }

        public String getModel() {
            return model;
        }

        public void setModel(String model) {
            this.model = model;
        }

        public String getFamily() {
            return family;
        }

        public void setFamily(String family) {
            this.family = family;
        }
    }


    /*
     * Utility functions
     */
    private static class Helpers{
        /*
         * Match algorithm using Regex Pattern
         * Logic: (Product Name) OR ((Manufacturer OR Family) AND Model)
         */
        static boolean isListingItemMatchToProduct(String listingItemJson,Product product){

            boolean matched=false;

            String regexPattern="";
            if(product.getProductName().isEmpty()==false){
                //lets tokenize product name
                String[] dividers = {" ","_","\\$","\\*"};//can include more
                String productNameTokens[] = tokenize(product.getProductName(),dividers);
                regexPattern += createExactANDRegexStr(productNameTokens);
            }

            if( (product.getManufacturer().isEmpty()==false || product.getFamily().isEmpty()==false) && product.getModel().isEmpty()==false){

                String  regexManuFacturerModelPattern= createExactORRegexStr(new String[]{product.getManufacturer(), product.getFamily()});

                if(!regexPattern.isEmpty()){
                    regexPattern="("+regexPattern+")|";
                }

                regexPattern +="((?=.*"+regexManuFacturerModelPattern+")(?=.*\\b"+product.getModel()+"\\b))";
            }

            if(regexPattern.isEmpty()==false){
                Pattern findPattern=Pattern.compile(regexPattern,Pattern.CASE_INSENSITIVE);
                Matcher patternMatcher = findPattern.matcher(listingItemJson);
                matched = patternMatcher.find();
            }

            return matched;
        }

        /*
         * create regex pattern using AND
         * (?=.*\bsample1\b) (?=.*\bsample2\b) (?=.*\bsample3\b)
         */
        static String createExactANDRegexStr(String[] keywords){

            if(keywords.length==0){
                return "";
            }

            StringBuilder regexPatternBuilder = new StringBuilder();
            for(String keyword: keywords){
                if(!keyword.isEmpty()) {
                    regexPatternBuilder.append("(?=.*\\b" + keyword + "\\b)");
                }
            }

            return regexPatternBuilder.toString();
        }

        /*
         * create regex pattern using OR
         * \bsample1\b | \bsample2\b | \bsample3\b
         */
        static String createExactORRegexStr(String[] keywords){

            if(keywords.length==0){
                return "";
            }

            StringBuilder regexPatternBuilder = new StringBuilder();
            for(String keyword: keywords){
                if(!keyword.isEmpty()) {
                    regexPatternBuilder.append("\\b"+keyword+"\\b|");
                }
            }

            return regexPatternBuilder.substring(0,regexPatternBuilder.length()-1).toString();
        }


        /*
        * split string into tokens using regex pattern series of OR words
        * \bsample1\b | \bsample2\b | \bsample3\b
        */
        static String[] tokenize(String text, String[] dividers){

            if(dividers.length==0){
                return new String[]{text};
            }

            StringBuilder regexPatternBuilder = new StringBuilder();
            for(String divider: dividers){
                regexPatternBuilder.append(divider+"|");
            }
            String regexPattern = regexPatternBuilder.substring(0, regexPatternBuilder.length() - 1).toString();

            return text.split(regexPattern);
        }

    }


}
