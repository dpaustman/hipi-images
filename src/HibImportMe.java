package org.hipi.tools;

import org.apache.hadoop.fs.*;
import org.hipi.imagebundle.HipiImageBundle;
import org.hipi.image.HipiImageHeader.HipiImageFormat;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.commons.cli.ParseException;

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Arrays;

public class HibImportMe {
    private static final Options options = new Options();
    private static final Parser parser = (Parser) new BasicParser();

    static {
        options.addOption("f", "force", false, "force overwrite if output HIB already exists");
        options.addOption("h", "hdfs-input", false, "assume input directory is on HDFS");
    }

    private static void usage() {
        // usage
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("hibImport.jar [options] <image directory> <output HIB>", options);
        System.exit(1);
    }

    public static void main(String[] args) throws IOException {

        // Attempt to parse the command line arguments
        CommandLine line = null;
        try {
            line = parser.parse(options, args);
        } catch (ParseException exp) {
            usage();
        }
        if (line == null) {
            usage();
        }

        String[] leftArgs = line.getArgs();
        if (leftArgs.length != 2) {
            usage();
        }

        String imageDir = leftArgs[0];
        String outputHib = leftArgs[1];

        boolean overwrite = false;
        if (line.hasOption("f")) {
            overwrite = true;
        }

        boolean hdfsInput = false;
        if (line.hasOption("h")) {
            hdfsInput = true;
        }

        System.out.println("Input image directory: " + imageDir);
        System.out.println("Input FS: " + (hdfsInput ? "HDFS" : "local FS"));
        System.out.println("Output HIB: " + outputHib);
        System.out.println("Overwrite HIB if it exists: " + (overwrite ? "true" : "false"));

        Configuration conf = new Configuration();

        if (hdfsInput) {
            HipiImageBundle hib = null;
            try {
                // create a hib instance
                hib = new HipiImageBundle(new Path(outputHib), conf);

                // add image to hib from HDFS fs.
                hib.openForWrite(overwrite);
                FileSystem fs = FileSystem.get(conf);
                addImagesFromHDFS(fs, hib, imageDir);
            } catch (IOException ex) {
                //ex.printStackTrace();
                System.err.println(ex);
                System.exit(10);
            } catch (NoSuchAlgorithmException ex) {
                //ex.printStackTrace();
                System.err.println(ex);
                System.exit(10);
            } catch (Exception ex) {
                System.err.println(ex);
                System.exit(10);
            } finally {
                if (hib != null) {
                    hib.close();
                }
            }
        } else {
            HipiImageBundle hib = null;
            try {
                // create a hib instance
                hib = new HipiImageBundle(new Path(outputHib), conf);

                // add images to hib from Local fs.
                hib.openForWrite(overwrite);
                addImagesFromLocal(hib, imageDir);
            } catch (IOException ex) {
                //ex.printStackTrace();
                System.err.println(ex);
                System.exit(10);
            } catch (NoSuchAlgorithmException ex) {
                //ex.printStackTrace();
                System.err.println(ex);
                System.exit(10);
            } finally {
                if (hib != null) {
                    hib.close();
                }
            }
        }

        System.out.println("Created: " + outputHib + " and " + outputHib + ".dat");
    }

    /**
     * FS
     * 递归遍历 Local FS 图片文件
     *
     * @param hib
     * @param imageDir
     */
    private static void addImagesFromLocal(HipiImageBundle hib, String imageDir) throws IOException, NoSuchAlgorithmException {
        File fileSource = new File(imageDir);
        if (fileSource.isDirectory()) { // Is Directory
            // 遍历文件夹
            File[] files = fileSource.listFiles();
            Arrays.sort(files);

            if (files == null) {
                System.err.println(String.format("Did not find any files in the local FS directory [%s]", imageDir));
                System.exit(0);
            }
            for (File f : files) {
                addImagesFromLocal(hib, f.getPath());
            }
        } else if (fileSource.isFile()) { // Is File
            // 添加图片到hib图片库
            FileInputStream fis = new FileInputStream(fileSource);
            String localPath = fileSource.getPath();

            // 添加图片 meta 信息
            HashMap<String, String> metaData = new HashMap<String, String>();
            metaData.put("source", localPath);
            String fileName = fileSource.getName().toLowerCase();
            metaData.put("filename", fileName);

            // 判断图片格式
            String suffix = fileName.substring(fileName.lastIndexOf('.'));
            if (suffix.compareTo(".jpg") == 0 || suffix.compareTo(".jpeg") == 0) {
                hib.addImage(fis, HipiImageFormat.JPEG, metaData);
                System.out.println(" ** added: " + fileName);
            } else if (suffix.compareTo(".png") == 0) {
                hib.addImage(fis, HipiImageFormat.PNG, metaData);
                System.out.println(" ** added: " + fileName);
            } else {
                System.out.println(" ** error: not supported - " + fileName);
            }
        } else { // Other
            System.out.println(" ** error: not supported - " + imageDir + " is not a file or directory.");
        }
    }

    /**
     * 递归遍历 HDFS 图片文件
     *
     * @param fs
     * @param hib
     * @param imageDir
     * @throws IOException
     */
    private static void addImagesFromHDFS(FileSystem fs, HipiImageBundle hib, String imageDir) throws IOException, NoSuchAlgorithmException {
        if (fs.isDirectory(new Path(imageDir))) { // Is Directory
            FileStatus[] files = fs.listStatus(new Path(imageDir));
            if (files == null) {
                System.err.println(String.format("Did not find any files in the HDFS directory [%s]", imageDir));
                System.exit(0);
            }
            Arrays.sort(files);

            for (FileStatus file : files) {
                addImagesFromHDFS(fs, hib, file.getPath().toString());
            }
        } else if (fs.isFile(new Path(imageDir))) { // Is File
            // 添加图片到hib图片库
            FileStatus file = fs.getFileStatus(new Path(imageDir));
            FSDataInputStream fdis = fs.open(file.getPath());
            String source = file.getPath().toString();

            // 添加图片 meta 信息
            HashMap<String, String> metaData = new HashMap<String, String>();
            metaData.put("source", source);

            // 判断图片格式
            String fileName = file.getPath().getName().toLowerCase();
            String suffix = fileName.substring(fileName.lastIndexOf('.'));
            if (suffix.compareTo(".jpg") == 0 || suffix.compareTo(".jpeg") == 0) {
                hib.addImage(fdis, HipiImageFormat.JPEG, metaData);
                System.out.println(" ** added: " + fileName);
            } else if (suffix.compareTo(".png") == 0) {
                hib.addImage(fdis, HipiImageFormat.PNG, metaData);
                System.out.println(" ** added: " + fileName);
            } else {
                System.out.println(" ** error: not supported - " + imageDir);
            }
        } else { // Other
            System.out.println(" ** error: not supported - " + imageDir + " is not a file or directory.");
        }
    }
}