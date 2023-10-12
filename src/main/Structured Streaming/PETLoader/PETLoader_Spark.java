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
/**
 * This class focuses on mapping methods from external libraries and then using that method to process the data.
 */
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

    /**
     * This function takes something that can't be automatically serialised and serialises it manually.
     */
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
    /**
     * This function is used to get the class.
     */
    private void generatePetClass(String className) throws MalformedURLException, ClassNotFoundException {
        URL jarUrl = new File(this.FileName).toURI().toURL();
        URLClassLoader classLoader = new URLClassLoader(new URL[]{jarUrl});
        PetClass = classLoader.loadClass(className);
    }

    /**
     * This function takes something that can't be automatically deserialized by automatic deserialization and manually does the deserialization.
     * Its input is the thing being serialised.
     */
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
    /**
     * This function is a constructor method.
     * This part is executed every time a new instance is created,
     * and this constructor method instantiates the desired class based on the input and finds the methods
     * that need to be used.
     */
    public PETLoader_Spark(String confPath, String Type, Integer id) throws Exception {
        this.confPath = confPath;
        this.Type = Type;
        this.id = id;
        initialize();
    }
    /**
     * What this function does is call an existing function that assigns a value to a member variable.
     * Access the json file based on the given address(confPath) and then parse the file to get the desired data.
     */
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
    /**
     * This function get FileName, FunctionName, ClassList, FunctionParameter, Default from id.
     */
    public void quickLoad() throws NoSuchFieldException, ClassNotFoundException, IllegalAccessException {
        JSONObject typeMethode = (JSONObject) PETLibrary.get(id.toString());
        FileName = "lib/" + (String) typeMethode.get("FileName");
        FunctionName = (String) typeMethode.get("FunctionName");
        ClassList = parseClassString((ArrayList<String>) typeMethode.get("ConstructorParameter"));
        FunctionParameter = parseClassString((ArrayList<String>) typeMethode.get("FunctionParameter"));
        Default = (ArrayList<Object>) typeMethode.get("Default");
    }
    /**
     * This function mainly takes a class name of type String and converts it into a Class type.
     * This obtains the desired data type.
     */
    public static Class[] parseClassString(ArrayList<String> InputList) throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
        int length = InputList.size();
        Class[] TmpList = new Class[length];
        for (int i = 0; i < length; i ++) {
            Class TmpClass = (Class.forName(InputList.get(i)));
            TmpList[i] = TmpClass;
        }
        return TmpList;
    }

    /**
     * This function is locating the class we need.
     */
    public void locateClass() throws Exception {
        classes = this.loadJarFile(FileName);
        int count = 0;
        for (Class c : classes){
            if (FunctionName.equals(c.getName())) break;
            count ++;
        }
        PetClass = classes.get(count);
    }

    /**
     * This function converts all String about the class to the Class type.
     */
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

    /**
     * This function takes all the String about the class inside the jar package and outputs it.
     */
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
    /**
     * This function mainly gets an instance of the class,
     * extracts the method that needs to be used in the instance, and then sets that method to be available.
     */
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
    /**
     * This method is mainly used when switching the id of the PET. By this method.
     * Another desired class will be located.
     */
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
        // Uncomment the following code to test the redeployment latency
//        id = newID;
//        initialize();
    }
//


    /**
     * This function is run the method, which belongs to the already instantiated class.
     */
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
