package com.cds.utils;

import org.springframework.util.Assert;
import org.springframework.util.StreamUtils;

import java.io.*;
import java.nio.channels.FileChannel;

public class FileCopyUtils {

    public static final int BUFFER_SIZE = StreamUtils.BUFFER_SIZE;


    //---------------------------------------------------------------------
    // Copy methods for java.io.File
    //---------------------------------------------------------------------

    /**
     * Copy the contents of the given input File to the given output File.
     *
     * @param in  the file to copy from
     * @param out the file to copy to
     * @return the number of bytes copied
     * @throws IOException in case of I/O errors
     */
    public static int copy(File in, File out) throws IOException {
        Assert.notNull(in, "No input File specified");
        Assert.notNull(out, "No output File specified");
        return copy(new BufferedInputStream(new FileInputStream(in)),
                new BufferedOutputStream(new FileOutputStream(out)));
    }

    /**
     * Copy the contents of the given byte array {@code in}to the given output File {@code out}.
     *
     * @param in  the byte array to copy from
     * @param out the file to copy to
     * @throws IOException in case of I/O errors
     */
    public static void copy(byte[] in, File out) throws IOException {
        Assert.notNull(in, "No input byte array specified");
        Assert.notNull(out, "No output File specified");
        ByteArrayInputStream inStream = new ByteArrayInputStream(in);
        OutputStream outStream = new BufferedOutputStream(new FileOutputStream(out));
        copy(inStream, outStream);
    }

    /**
     * Copy the contents of the given input File into a new byte array.
     *
     * @param in the file to copy from
     * @return the new byte array that has been copied to
     * @throws IOException in case of I/O errors
     */
    public static byte[] copyToByteArray(File in) throws IOException {
        Assert.notNull(in, "No input File specified");
        return copyToByteArray(new BufferedInputStream(new FileInputStream(in)));
    }


    //---------------------------------------------------------------------
    // Copy methods for java.io.InputStream / java.io.OutputStream
    //---------------------------------------------------------------------

    /**
     * Copy the contents of the given InputStream to the given OutputStream.
     * Closes both streams when done.
     *
     * @param in  the stream to copy from
     * @param out the stream to copy to
     * @return the number of bytes copied
     * @throws IOException in case of I/O errors
     */
    public static int copy(InputStream in, OutputStream out) throws IOException {
        Assert.notNull(in, "No InputStream specified");
        Assert.notNull(out, "No OutputStream specified");
        try {
            return StreamUtils.copy(in, out);
        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            try {
                out.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * Copy the contents of the given byte array {@code in} to the given OutputStream {@code out}.
     * Closes the stream when done.
     *
     * @param in  the byte array to copy from
     * @param out the OutputStream to copy to
     * @throws IOException in case of I/O errors
     */
    public static void copy(byte[] in, OutputStream out) throws IOException {
        Assert.notNull(in, "No input byte array specified");
        Assert.notNull(out, "No OutputStream specified");
        out.write(in);
        out.close();
//        try {
//            out.write(in);
//        }
//        finally {
//            try {
//                out.close();
//            }
//            catch (IOException ex) {
//            }
//        }
    }

    /**
     * Copy the contents of the given InputStream {@code in} into a new byte array.
     * Closes the stream when done.
     *
     * @param in the stream to copy from
     * @return the new byte array that has been copied to
     * @throws IOException in case of I/O errors
     */
    public static byte[] copyToByteArray(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(BUFFER_SIZE);
        copy(in, out);
        return out.toByteArray();
    }


    //---------------------------------------------------------------------
    // Copy methods for java.io.Reader / java.io.Writer
    //---------------------------------------------------------------------

    /**
     * Copy the contents of the given Reader to the given Writer.
     * Closes both when done.
     *
     * @param in  the Reader to copy from
     * @param out the Writer to copy to
     * @return the number of characters copied
     * @throws IOException in case of I/O errors
     */
    public static int copy(Reader in, Writer out) throws IOException {
        Assert.notNull(in, "No Reader specified");
        Assert.notNull(out, "No Writer specified");
        try {
            int byteCount = 0;
            char[] buffer = new char[BUFFER_SIZE];
            int bytesRead = -1;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
                byteCount += bytesRead;
            }
            out.flush();
            return byteCount;
        } finally {
            try {
                in.close();
            } catch (IOException ex) {
            }
            try {
                out.close();

            } catch (IOException ex) {
            }
        }
    }

    /**
     * Copy the contents of the given String to the given output Writer.
     * Closes the writer when done.
     *
     * @param in  the String to copy from
     * @param out the Writer to copy to
     * @throws IOException in case of I/O errors
     */
    public static void copy(String in, Writer out) throws IOException {
        Assert.notNull(in, "No input String specified");
        Assert.notNull(out, "No Writer specified");
        try {
            out.write(in);
        } finally {
            try {
                out.close();
            } catch (IOException ex) {
            }
        }
    }

    /**
     * Copy the contents of the given Reader into a String.
     * Closes the reader when done.
     *
     * @param in the reader to copy from
     * @return the String that has been copied to
     * @throws IOException in case of I/O errors
     */
    public static String copyToString(Reader in) throws IOException {
        StringWriter out = new StringWriter();
        copy(in, out);
        return out.toString();
    }


    //-------------------------file system copy---------------------------//

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void copyDirectory(String srcDir, String tarDir) {
        Assert.notNull(srcDir, "the srcDir must not be null!");
        Assert.notNull(tarDir, "the tarDir must not be null!");

        File srcFile = new File(srcDir);
        if (srcFile.isDirectory()) {
            File tarFile = new File(tarDir);
            if (!tarFile.exists()) {
                tarFile.mkdir();
            }
            File[] files = srcFile.listFiles();
            if (files != null) {
                for (File fs : files) {
                    copyDirectory(fs.toString(), tarDir + File.separator + fs.getName());
                }
            }

        } else {
            copyFileByChannel(srcDir, tarDir);
        }
    }

    /**
     * copy file
     * nio
     *
     * @param srcFile src file
     * @param tarFile target file
     */
    public static void copyFileByChannel(String srcFile, String tarFile) {
        try {
            FileInputStream fi = new FileInputStream(srcFile);
            FileOutputStream fo = new FileOutputStream(tarFile);
            FileChannel in = fi.getChannel();
            FileChannel out = fo.getChannel();
            in.transferTo(0, in.size(), out);
            fi.close();
            in.close();
            fo.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Delete the supplied {@link File} - for directories,
     * recursively delete any nested directories or files as well.
     *
     * @param root the root {@code File} to delete
     * @return {@code true} if the {@code File} was deleted,
     * otherwise {@code false}
     */
    public static boolean deleteRecursively(File root) {
        if (root != null && root.exists()) {
            if (root.isDirectory()) {
                File[] children = root.listFiles();
                if (children != null) {
                    for (File child : children) {
                        deleteRecursively(child);
                    }
                }
            }
            return root.delete();
        }
        return false;
    }

    /**
     * Recursively copy the contents of the {@code src} file/directory
     * to the {@code dest} file/directory.
     *
     * @param src  the source directory
     * @param dest the destination directory
     * @throws IOException in the case of I/O errors
     */
    public static void copyRecursively(File src, File dest) throws IOException {
        Assert.isTrue(src != null && (src.isDirectory() || src.isFile()), "Source File must denote a directory or file");
        Assert.notNull(dest, "Destination File must not be null");
        doCopyRecursively(src, dest);
    }

    /**
     * Actually copy the contents of the {@code src} file/directory
     * to the {@code dest} file/directory.
     *
     * @param src  the source directory
     * @param dest the destination directory
     * @throws IOException in the case of I/O errors
     */
    private static void doCopyRecursively(File src, File dest) throws IOException {
        if (src.isDirectory()) {
            dest.mkdir();
            File[] entries = src.listFiles();
            if (entries == null) {
                throw new IOException("Could not list files in directory: " + src);
            }
            for (File entry : entries) {
                doCopyRecursively(entry, new File(dest, entry.getName()));
            }
        } else if (src.isFile()) {
            try {
                dest.createNewFile();
            } catch (IOException ex) {
                IOException ioex = new IOException("Failed to create file: " + dest);
                ioex.initCause(ex);
                throw ioex;
            }
            FileCopyUtils.copy(src, dest);
        } else {
            // Special File handle: neither a file not a directory.
            // Simply skip it when contained in nested directory...
        }
    }

}
