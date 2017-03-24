package com.cds.utils;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.Writer;
import java.math.BigInteger;
import java.net.FileNameMap;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * Created by cds on 11/20/16 19:03.
 * wrap file operation
 */
public final class FileUtils {

    /**
     * 文件大小单位：1KB。
     */
    public static final long ONE_KB = 1024;

    /**
     * 文件大小单位：1MB。
     */
    public static final long ONE_MB = ONE_KB * ONE_KB;

    /**
     * 文件大小单位：1GB。
     */
    public static final long ONE_GB = ONE_KB * ONE_MB;


    /**
     * Buffer的大小
     */
    private static Integer BUFFER_SIZE = 1024 * 1024 * 10;

    public static MessageDigest MD5 = null;

    static {
        try {
            MD5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ne) {
            ne.printStackTrace();
        }
    }

    /**
     * 获取文件的md5
     * @param file
     * @return
     */
    public static String fileMD5(File file) {
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(file);
            byte[] buffer = new byte[8192];
            int length;
            while ((length = fileInputStream.read(buffer)) != -1) {
                MD5.update(buffer, 0, length);
            }
            return new BigInteger(1, MD5.digest()).toString(16);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                if (fileInputStream != null)
                    fileInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * 获取文件的行数
     *
     * @param file 统计的文件
     * @return 文件行数
     */
    public final static int countLines(File file) {
        try(LineNumberReader rf = new LineNumberReader(new FileReader(file))){
            long fileLength = file.length();
            rf.skip(fileLength);
            return rf.getLineNumber();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 以列表的方式获取文件的所有行
     *
     * @param file 需要出来的文件
     * @return 包含所有行的list
     */
    public final static List<String> lines(File file) {
        List<String> list = new ArrayList<>();
        try (
                BufferedReader reader = new BufferedReader(new FileReader(file))
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                list.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 以列表的方式获取文件的所有行
     *
     * @param file     需要处理的文件
     * @param encoding 指定读取文件的编码
     * @return 包含所有行的list
     */
    public final static List<String> lines(File file, String encoding) {
        List<String> list = new ArrayList<>();
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), encoding))
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                list.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 以列表的方式获取文件的指定的行数数据
     *
     * @param file  处理的文件
     * @param lines 需要读取的行数
     * @return 包含制定行的list
     */
    public final static List<String> lines(File file, int lines) {
        List<String> list = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                list.add(line);
                if (list.size() == lines) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 以列表的方式获取文件的指定的行数数据
     *
     * @param file     需要处理的函数
     * @param lines    需要处理的行还俗
     * @param encoding 指定读取文件的编码
     * @return 包含制定行的list
     */
    public final static List<String> lines(File file, int lines, String encoding) {
        List<String> list = new ArrayList<>();
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), encoding))
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                list.add(line);
                if (list.size() == lines) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 在文件末尾追加一行
     *
     * @param file 需要处理的函数
     * @param str  添加的子字符串
     * @return 是否成功
     */
    public final static boolean appendLine(File file, String str) {
        try (
                RandomAccessFile randomFile = new RandomAccessFile(file, "rw")
        ) {
            long fileLength = randomFile.length();
            randomFile.seek(fileLength);
            randomFile.writeBytes(SysUtils.FILE_SEPARATOR + str);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 在文件末尾追加一行
     *
     * @param file     需要处理的文件
     * @param str      添加的字符串
     * @param encoding 指定写入的编码
     * @return 是否成功
     */
    public final static boolean appendLine(File file, String str, String encoding) {
        String lineSeparator = System.getProperty("line.separator", "\n");
        try (
                RandomAccessFile randomFile = new RandomAccessFile(file, "rw")
        ) {
            long fileLength = randomFile.length();
            randomFile.seek(fileLength);
            randomFile.write((lineSeparator + str).getBytes(encoding));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 将字符串写入到文件中
     */
    public final static boolean write(File file, String str) {
        try (
                RandomAccessFile randomFile = new RandomAccessFile(file, "rw")
        ) {
            randomFile.writeBytes(str);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 将字符串以追加的方式写入到文件中
     */
    public final static boolean writeAppend(File file, String str) {
        try (
                RandomAccessFile randomFile = new RandomAccessFile(file, "rw")
        ) {
            long fileLength = randomFile.length();
            randomFile.seek(fileLength);
            randomFile.writeBytes(str);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 将字符串以制定的编码写入到文件中
     */
    public final static boolean write(File file, String str, String encoding) {
        try (
                RandomAccessFile randomFile = new RandomAccessFile(file, "rw")
        ) {
            randomFile.write(str.getBytes(encoding));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 将字符串以追加的方式以制定的编码写入到文件中
     */
    public final static boolean writeAppend(File file, String str, String encoding) {
        try (
                RandomAccessFile randomFile = new RandomAccessFile(file, "rw")
        ) {
            long fileLength = randomFile.length();
            randomFile.seek(fileLength);
            randomFile.write(str.getBytes(encoding));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 快速清空一个超大的文件
     *
     * @param file 需要处理的文件
     * @return 是否成功
     */
    public final static boolean cleanFile(File file) {
        try (
                FileWriter fw = new FileWriter(file)
        ) {
            fw.write("");
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 获取文件的Mime类型
     *
     * @param file 需要处理的文件
     * @return 返回文件的mime类型
     * @throws IOException
     */
    public final static String mimeType(String file) throws IOException {
        FileNameMap fileNameMap = URLConnection.getFileNameMap();
        return fileNameMap.getContentTypeFor(file);
    }

    /**
     * 获取文件的类型
     * <p/>
     * Summary:只利用文件头做判断故不全
     *
     * @param file 需要处理的文件
     * @return 文件类型
     */
    public final static String fileType(File file) {
        return FileTypeImpl.getFileType(file);
    }

    /**
     * 获取文件最后的修改时间
     *
     * @param file 需要处理的文件
     * @return 返回文件的修改时间
     */
    public final static Date modifyTime(File file) {
        return new Date(file.lastModified());
    }




    /**
     * 复制文件
     *
     * @param resourcePath 源文件
     * @param targetPath   目标文件
     * @return 是否成功
     */
    public final static boolean copy(String resourcePath, String targetPath) {
        File file = new File(resourcePath);
        return copy(file, targetPath);
    }

    /**
     * 复制文件
     * 通过该方式复制文件文件越大速度越是明显
     *
     * @param file       需要处理的文件
     * @param targetFile 目标文件
     * @return 是否成功
     */
    public final static boolean copy(File file, String targetFile) {
        try (
                FileInputStream fin = new FileInputStream(file);
                FileOutputStream fout = new FileOutputStream(new File(targetFile))
        ) {
            FileChannel in = fin.getChannel();
            FileChannel out = fout.getChannel();
            //设定缓冲区
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
            while (in.read(buffer) != -1) {
                //准备写入，防止其他读取，锁住文件
                buffer.flip();
                out.write(buffer);
                //准备读取。将缓冲区清理完毕，移动文件内部指针
                buffer.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }



    /**
     * 利用简单的文件头字节特征探测文件编码
     *
     * @param file 需要处理的文件
     * @return UTF-8 Unicode UTF-16BE GBK
     */
    public final static String simpleEncoding(String file) {
        try {
            return FileImpl.simpleEncoding(file);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 创建多级目录
     *
     * @param paths 需要创建的目录
     * @return 是否成功
     */
    public final static boolean createPaths(String paths) {
        File dir = new File(paths);
        return !dir.exists() && dir.mkdir();
    }

    /**
     * 创建文件支持多级目录
     *
     * @param filePath 需要创建的文件
     * @return 是否成功
     */
    public final static boolean createFiles(String filePath) {
        File file = new File(filePath);
        File dir = file.getParentFile();
        if (!dir.exists()) {
            if (dir.mkdirs()) {
                try {
                    return file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    /**
     * 删除一个文件
     *
     * @param file 需要处理的文件
     * @return 是否成功
     */
    public final static boolean deleteFile(File file) {
        return file.delete();
    }

    /**
     * 删除一个目录
     *
     * @param file 需要处理的文件
     * @return 是否成功
     */
    public final static boolean deleteDir(File file) {
        List<File> files = listFileAll(file);
        if (ValidateUtils.valid(files)) {
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteDir(f);
                } else {
                    deleteFile(f);
                }
            }
        }
        return file.delete();
    }


    /**
     * 快速的删除超大的文件
     *
     * @param file 需要处理的文件
     * @return 是否成功
     */
    public final static boolean deleteBigFile(File file) {
        return cleanFile(file) && file.delete();
    }


    /**
     * 复制目录
     *
     * @param filePath   需要处理的文件
     * @param targetPath 目标文件
     */
    public final static void copyDir(String filePath, String targetPath) {
        File file = new File(filePath);
        copyDir(file, targetPath);
    }

    /**
     * 复制目录
     *
     * @param filePath   需要处理的文件
     * @param targetPath 目标文件
     */
    public final static void copyDir(File filePath, String targetPath) {
        File targetFile = new File(targetPath);
        if (!targetFile.exists()) {
            createPaths(targetPath);
        }
        File[] files = filePath.listFiles();
        if (ValidateUtils.valid(files)) {
            for (File file : files) {
                String path = file.getName();
                if (file.isDirectory()) {
                    copyDir(file, targetPath + "/" + path);
                } else {
                    copy(file, targetPath + "/" + path);
                }
            }
        }
    }

    /**
     * 罗列指定路径下的全部文件
     *
     * @param path 需要处理的文件
     * @return 包含所有文件的的list
     */
    public final static List<File> listFile(String path) {
        File file = new File(path);
        return listFile(file);
    }

    /**
     * 罗列指定路径下的全部文件
     * @param path 需要处理的文件
     * @param child 是否罗列子文件
     * @return 包含所有文件的的list
     */
    public final static List<File> listFile(String path,boolean child){
        return listFile(new File(path),child);
    }


    /**
     * 罗列指定路径下的全部文件
     *
     * @param path 需要处理的文件
     * @return 返回文件列表
     */
    public final static List<File> listFile(File path) {
        List<File> list = new ArrayList<>();
        File[] files = path.listFiles();
        if (ValidateUtils.valid(files)) {
            for (File file : files) {
                if (file.isDirectory()) {
                    list.addAll(listFile(file));
                } else {
                    list.add(file);
                }
            }
        }
        return list;
    }

    /**
     * 罗列指定路径下的全部文件
     * @param path 指定的路径
     * @param child 是否罗列子目录
     * @return
     */
    public final static List<File> listFile(File path,boolean child){
        List<File> list = new ArrayList<>();
        File[] files = path.listFiles();
        if (ValidateUtils.valid(files)) {
            for (File file : files) {
                if (child && file.isDirectory()) {
                    list.addAll(listFile(file));
                } else {
                    list.add(file);
                }
            }
        }
        return list;
    }

    /**
     * 罗列指定路径下的全部文件包括文件夹
     *
     * @param path 需要处理的文件
     * @return 返回文件列表
     */
    public final static List<File> listFileAll(File path) {
        List<File> list = new ArrayList<>();
        File[] files = path.listFiles();
        if (ValidateUtils.valid(files)) {
            for (File file : files) {
                list.add(file);
                if (file.isDirectory()) {
                    list.addAll(listFileAll(file));
                }
            }
        }
        return list;
    }

    /**
     * 罗列指定路径下的全部文件包括文件夹
     *
     * @param path   需要处理的文件
     * @param filter 处理文件的filter
     * @return 返回文件列表
     */
    public final static List<File> listFileFilter(File path, FilenameFilter filter) {
        List<File> list = new ArrayList<>();
        File[] files = path.listFiles();
        if (ValidateUtils.valid(files)) {
            for (File file : files) {
                if (file.isDirectory()) {
                    list.addAll(listFileFilter(file, filter));
                } else {
                    if (filter.accept(file.getParentFile(), file.getName())) {
                        list.add(file);
                    }
                }
            }
        }
        return list;
    }

    /**
     * 获取指定目录下的特点文件,通过后缀名过滤
     *
     * @param dirPath  需要处理的文件
     * @param postfixs 文件后缀
     * @return 返回文件列表
     */
    public final static List<File> listFileFilter(File dirPath, final String postfixs) {
        /*
        如果在当前目录中使用Filter讲只罗列当前目录下的文件不会罗列孙子目录下的文件
        FilenameFilter filefilter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(postfixs);
            }
        };
        */
        List<File> list = new ArrayList<File>();
        File[] files = dirPath.listFiles();
        if (ValidateUtils.valid(files)) {
            for (File file : files) {
                if (file.isDirectory()) {
                    list.addAll(listFileFilter(file, postfixs));
                } else {
                    String fileName = file.getName().toLowerCase();
                    if (fileName.endsWith(postfixs.toLowerCase())) {
                        list.add(file);
                    }
                }
            }
        }
        return list;
    }

    /**
     * 在指定的目录下搜寻文个文件
     *
     * @param dirPath  搜索的目录
     * @param fileName 搜索的文件名
     * @return 返回文件列表
     */
    public final static List<File> searchFile(File dirPath, String fileName) {
        List<File> list = new ArrayList<>();
        File[] files = dirPath.listFiles();
        if (ValidateUtils.valid(files)) {
            for (File file : files) {
                if (file.isDirectory()) {
                    list.addAll(searchFile(file, fileName));
                } else {
                    String Name = file.getName();
                    if (Name.equals(fileName)) {
                        list.add(file);
                    }
                }
            }
        }
        return list;
    }

    /**
     * 查找符合正则表达式reg的的文件
     *
     * @param dirPath 搜索的目录
     * @param reg     正则表达式
     * @return 返回文件列表
     */
    public final static List<File> searchFileReg(File dirPath, String reg) {
        List<File> list = new ArrayList<>();
        File[] files = dirPath.listFiles();
        if (ValidateUtils.valid(files)) {
            for (File file : files) {
                if (file.isDirectory()) {
                    list.addAll(searchFile(file, reg));
                } else {
                    String Name = file.getName();
                    if (RegUtils.isMatche(Name, reg)) {
                        list.add(file);
                    }
                }
            }
        }
        return list;
    }


    /**
     * 获取文件后缀名
     * @param file
     * @return
     */
    public final static String suffix(File file){
        String fileName=file.getName();
        return fileName.substring(fileName.indexOf(".")+1);
    }



    /**
     * 递归取得某个目录下所有的文件
     *
     * @param path  目录
     * @return 文件List
     */
    public static List<File> getNestedFiles(String path) {
        File directory = new File(path);
        if (!directory.exists() || !directory.isDirectory()) {
            throw new IllegalArgumentException("Nonexistent directory[" + path + "]");
        }

        return new Recursiver().getFileList(directory);
    }

    /**
     * 把字符串写到文件中
     *
     * @param path  文件路径
     * @param str   字符串
     * @param append  是否追加，否的话会覆盖原来的内容
     */
    public static void writeString(String path, String str, boolean append) {
        FileWriter out = null;
        try {
            out = new FileWriter(path, append);
            out.write(str);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not write String[" + path + "]", e);
        }
        finally {
            close(out);
        }
    }

    /**
     * 从文件中读取字符串，使用默认字符集
     *
     * @param path   文件路径
     * @return 文件内容的字符串
     */
    public static String readString(String path) {
        FileInputStream in = null;
        ByteArrayOutputStream out = null;

        try {
            in = new FileInputStream(path);
            out = new ByteArrayOutputStream();

            byte[] buffer = new byte[BUFFER_SIZE];
            int length;
            while ((length = in.read(buffer)) != -1) {
                out.write(buffer, 0, length);
            }
            return new String(out.toByteArray());
        }
        catch (IOException e) {
            throw new RuntimeException("Could not read String[" + path + "]", e);
        }
        finally {
            close(in);
            close(out);
        }
    }

    /**
     * 从输入流中读取字符串，使用默认字符集
     *
     * @param in
     *            输入流
     * @return 流内容的字符串
     */
    public static String readString(InputStream in) {
        ByteArrayOutputStream out = null;

        try {
            out = new ByteArrayOutputStream();

            byte[] buffer = new byte[BUFFER_SIZE];
            int length;
            while ((length = in.read(buffer)) != -1) {
                out.write(buffer, 0, length);
            }
            return new String(out.toByteArray());
        }
        catch (IOException e) {
            throw new RuntimeException("Could not read stream", e);
        }
        finally {
            close(in);
            close(out);
        }
    }

    /**
     * 读取指定路径的Properties文件
     *
     * @param path
     *            路径
     * @return Properties对象
     */
    public static Properties readProperties(String path) {
        File file = new File(path);
        if (!file.exists()) {
            return null;
        }

        Properties properties = new Properties();
        InputStream in = null;

        try {
            in = new FileInputStream(file);
            properties.load(in);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not read Properties[" + path + "]", e);
        }
        finally {
            close(in);
        }

        return properties;
    }

    /**
     * 从输入流读取Properties对象
     *
     * @param in
     *            输入流
     * @return Properties对象
     */
    public static Properties readProperties(InputStream in) {
        Properties properties = new Properties();

        try {
            properties.load(in);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not read Properties", e);
        }
        finally {
            close(in);
        }

        return properties;
    }

    /**
     * 把Properties对象写到指定路径的文件里
     *
     * @param path
     *            路进
     * @param properties
     *            Properties对象
     */
    public static void writeProperties(String path, Properties properties) {
        OutputStream out = null;

        try {
            out = new FileOutputStream(path);
            properties.store(out, null);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not write Properties[" + path + "]", e);
        }
        finally {
            close(out);
        }
    }

    /**
     * 关闭输入流
     *
     * @param in
     *            输入流
     */
    public static void close(InputStream in) {
        try {
            if (in != null) {
                in.close();
            }
        }
        catch (IOException e) {
        }
    }

    /**
     * 关闭输出流
     *
     * @param out
     *            输出流
     */
    public static void close(OutputStream out) {
        try {
            if (out != null) {
                out.close();
            }
        }
        catch (IOException e) {
        }
    }

    /**
     * 关闭 Reader
     *
     * @param in
     *            Reader
     */
    public static void close(Reader in) {
        try {
            if (in != null) {
                in.close();
            }
        }
        catch (IOException e) {
        }
    }

    /**
     * 关闭 Writer
     *
     * @param out
     *            Writer
     */
    public static void close(Writer out) {
        try {
            if (out != null) {
                out.close();
            }
        }
        catch (IOException e) {
        }
    }

    /**
     * 关闭 Closeable 对象。
     *
     * @param closeable
     *            Closeable 对象
     */
    public static void close(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        }
        catch (IOException e) {
        }
    }

    /**
     * 取得文件的后缀名。
     *
     * @param fileName
     *            文件名
     * @return 后缀名
     */
    public static String getExtension(String fileName) {
        if (ValidateUtils.isEmpty(fileName)) {
            return null;
        }

        int pointIndex = fileName.lastIndexOf(".");
        return pointIndex > 0 && pointIndex < fileName.length() ? fileName.substring(pointIndex + 1).toLowerCase()
                : null;
    }

    /**
     * 复制给定的数组中的内容到输出流中。
     *
     * @param in
     *            需要复制的数组
     * @param out
     *            复制到的输出流
     * @throws IOException
     *             当发生 I/O 异常时抛出
     */
    public static void copy(byte[] in, OutputStream out) throws IOException {
        try {
            out.write(in);
        }
        finally {
            try {
                out.close();
            }
            catch (IOException ex) {
            }
        }
    }

    /**
     * 将以 byte 为单位的文件大小转换为一个可读性更好的文件大小，最终结果精确到一位小数。
     *
     * @param size
     *            以 byte 为单位的文件大小
     * @return 更具可读性的文件大小（包括单位：GB、MB、KB、B），例如：102 B、1.5 KB、23.8 MB、34.2 GB
     */
    public static String byteCountToDisplaySize(long size) {
        String displaySize;

        if (size / ONE_GB > 0) {
            displaySize = MathUtils.div(size * 1.0, ONE_GB, 1) + " GB";
        }
        else if (size / ONE_MB > 0) {
            displaySize = MathUtils.div(size * 1.0, ONE_MB, 1) + " MB";
        }
        else if (size / ONE_KB > 0) {
            displaySize = MathUtils.div(size * 1.0, ONE_KB, 1) + " KB";
        }
        else {
            displaySize = String.valueOf(size) + " B";
        }

        return displaySize;
    }

    private static class Recursiver {

        private static ArrayList<File> files = new ArrayList<File>();

        public List<File> getFileList(File file) {
            File children[] = file.listFiles();

            for (int i = 0; i < children.length; i++) {
                if (children[i].isDirectory()) {
                    new Recursiver().getFileList(children[i]);
                }
                else {
                    files.add(children[i]);
                }
            }

            return files;
        }
    }

}
