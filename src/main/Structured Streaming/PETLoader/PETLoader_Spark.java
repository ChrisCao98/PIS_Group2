package PETLoader;

import alg.SS03;
import alg.SS03$;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

public class PETLoader_Spark<T> implements Serializable {
    private String Home; // directory of the root
    private String Type;
    private Integer id;
    private String confPath;
    private String ConfPath; // directory of the Configuration file, input
    private String FileName; // The name of the package
    private String FunctionName; // The name of the PET methode
    private int size; // How many PET are there for this kind of data type
    private transient ArrayList<Class> classes;
    private JSONObject PETLibrary;

    private Class[] ClassList;
    private Class[] FunctionParameter;
    private ArrayList<Object> Default;
    private transient Object CurrentPolicy;
    private transient Method process;
    private transient Class PetClass;
    private Object[] objects;
//
//    public Integer getId(){
//        return id;
//    }

    //reconstruct for dynamic add or remove stream
//    private SparkSession sparkSession = SS03$.MODULE$.getSpark();
//
//    public SparkSession getSparkSession() {
//
//
//        return sparkSession;
//    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
//        if (classes != null) {
//            for (Class clazz : classes) {
//                out.writeObject(clazz);
//            }
//        }
        if (PetClass != null) {
            String className = PetClass.getName();
            out.writeObject(className);
        }

//        out.writeObject(process.getDeclaringClass());
//        out.writeUTF(process.getName());
//        out.writeObject(process.getParameterTypes());
//        out.writeObject(CurrentPolicy);
    }
    private void generatePetClass(String className) throws MalformedURLException, ClassNotFoundException {
        URL jarUrl = new File(this.FileName).toURI().toURL();
        URLClassLoader classLoader = new URLClassLoader(new URL[]{jarUrl});
        PetClass = classLoader.loadClass(className);
    }
    private void readObject(ObjectInputStream in) throws IOException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        in.defaultReadObject();
//        for (int i = 0; i < size; i++) {
//            Class clazz = (Class) in.readObject();
//            classes.add(clazz);
//        }
        String className = (String) in.readObject();

        generatePetClass(className);


//        Class<?> declaringClass = (Class<?>) in.readObject();
//        String methodName = in.readUTF();
//        Class<?>[] parameterTypes = (Class<?>[]) in.readObject();
//        CurrentPolicy = in.readObject();
//        try {
//            process = declaringClass.getMethod(methodName, parameterTypes);
//        } catch (Exception e) {
//            throw new IOException(String.format("Error occurred resolving deserialized method '%s.%s'", declaringClass.getSimpleName(), methodName), e);
//        }

        CurrentPolicy = PetClass.getConstructor(ClassList).newInstance(objects);

        process = PetClass.getMethod("process", FunctionParameter);

//

    }
    public PETLoader_Spark(String confPath, String Type, Integer id) throws Exception {
        this.confPath = confPath;
        this.Type = Type;
        this.id = id;
        initialize();
    }

    public void initialize() throws Exception {

        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader(confPath));
            // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
            JSONObject jsonObject = (JSONObject) obj;
            Home = (String) jsonObject.get("HOMEDIR");
            PETLibrary = (JSONObject) jsonObject.get(Type);
            size = PETLibrary.size();
            quickLoad();
        } catch (Exception e) {
            e.printStackTrace();
        }
        ConfPath = confPath;
        locateClass();

    }
//
    public void quickLoad() throws NoSuchFieldException, ClassNotFoundException, IllegalAccessException {
        JSONObject typeMethode = (JSONObject) PETLibrary.get(id.toString());
        FileName = "lib/" + (String) typeMethode.get("FileName");
        FunctionName = (String) typeMethode.get("FunctionName");
        ClassList = parseClassString((ArrayList<String>) typeMethode.get("ConstructorParameter"));
        FunctionParameter = parseClassString((ArrayList<String>) typeMethode.get("FunctionParameter"));
        Default = (ArrayList<Object>) typeMethode.get("Default");
    }
    public static Class[] parseClassString(ArrayList<String> InputList) throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
        int length = InputList.size();
        Class[] TmpList = new Class[length];
        for (int i = 0; i < length; i ++) {
            Class TmpClass = (Class.forName(InputList.get(i)));
            TmpList[i] = TmpClass;
        }
        return TmpList;
    }
    public void locateClass() throws Exception {
        classes = this.loadJarFile(FileName);
        int count = 0;
        for (Class c : classes){
            if (FunctionName.equals(c.getName())) break;
            count ++;
        }
        PetClass = classes.get(count);
    }


    //ArrayList<Class>
    private ArrayList<Class> loadJarFile(String filePath) throws Exception {

        ArrayList<Class> availableClasses = new ArrayList<>();

        ArrayList<String> classNames = getClassNamesFromJar(filePath);
        File f = new File(filePath);

        URLClassLoader classLoader = new URLClassLoader(new URL[]{f.toURI().toURL()});
        for (String className : classNames) {
            try {
                Class cc = classLoader.loadClass(className);
                availableClasses.add(cc);
            } catch (ClassNotFoundException e) {
                System.out.println("Class " + className + " was not found!");
            }
        }
        return availableClasses;
    }
    private ArrayList<String> getClassNamesFromJar(String jarPath) throws Exception {
        return getClassNamesFromJar(new JarInputStream(new FileInputStream(jarPath)));
    }
    private ArrayList<String> getClassNamesFromJar(JarInputStream jarFile) throws Exception {
        ArrayList<String> classNames = new ArrayList<>();
        try {
            //JarInputStream jarFile = new JarInputStream(jarFileStream);
            JarEntry jar;

            //Iterate through the contents of the jar file
            while (true) {
                jar = jarFile.getNextJarEntry();
                if (jar == null) {
                    break;
                }
                //Pick file that has the extension of .class
                if ((jar.getName().endsWith(".class"))) {
                    String className = jar.getName().replaceAll("/", "\\.");
                    String myClass = className.substring(0, className.lastIndexOf('.'));
                    classNames.add(myClass);
                }
            }
        } catch (Exception e) {
            throw new Exception("Error while getting class names from jar", e);
        }
        return classNames;
    }
//
    public void instantiate() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        objects = new Object[ClassList.length];
        for (int i = 0; i < ClassList.length; i++) {
            Class aClass = ClassList[i];
            Object o = Default.get(i);
            if (aClass.isInstance(o)) {
                Object cast = aClass.cast(o);
                objects[i] = cast;
            } else {
                Constructor declaredConstructor = aClass.getConstructor(String.class);
                Object cast = declaredConstructor.newInstance(o.toString());
                objects[i] = cast;
            }
        }

        CurrentPolicy = PetClass.getConstructor(ClassList).newInstance(objects);
//        CurrentPolicy = PetClass.getConstructor(ClassList).newInstance(Default.toArray(new Object[Default.size()]));
        process = PetClass.getMethod("process", FunctionParameter);

        process.setAccessible(true);

//        System.out.println("good4");
//        PETMethod = new SerializableMethod<T>(process, CurrentPolicy);
//        System.out.println("good5");
    }
    public void reloadPET(Integer newID) throws Exception {
        if (newID < 0 ){
            throw new IllegalArgumentException();
        } else if (newID >= size) {
            System.out.println("PET ID out of bound! Reloading PET Library");
            initialize();
        }else {
            System.out.println("Policy switched to " + newID);
            id = newID;
            quickLoad();
            locateClass();
        }
    }
//

//    public ArrayList<Object> invoke(Object input) throws InvocationTargetException, IllegalAccessException {
////        return (ArrayList<T>) process.invoke(CurrentPolicy, input);
//        System.out.println("already in invoke");
//        return (ArrayList<Object>) process.invoke(CurrentPolicy,input);
//    }
//

    public ArrayList<T> invoke(T input) throws InvocationTargetException, IllegalAccessException {
        if(process==null){
            System.out.println("process is null!!!!!");
        }
        if(CurrentPolicy==null){
            System.out.println("CurrentPolicy is null!!!!!");
        }
        return (ArrayList<T>) process.invoke(CurrentPolicy,input);
    }
    public void setId(Integer id) throws Exception {
        this.id = id;
        initialize();
    }



    public String getHome() {
        return Home;
    }

    public void setHome(String home) {
        Home = home;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getType() {
        return Type;
    }

    public void setType(String type) {
        Type = type;
    }

}
