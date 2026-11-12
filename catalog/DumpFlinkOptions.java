import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.jar.*;

/** Dumps every public static ConfigOption found in the given jars as JSON lines. */
public class DumpFlinkOptions {
    static String esc(Object o) {
        if (o == null) return "null";
        String s = o.toString().replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\t", " ");
        return "\"" + s + "\"";
    }
    public static void main(String[] args) throws Exception {
        Class<?> co = Class.forName("org.apache.flink.configuration.ConfigOption");
        Field clazzF = co.getDeclaredField("clazz"); clazzF.setAccessible(true);
        Field listF = co.getDeclaredField("isList"); listF.setAccessible(true);
        Method fb = co.getMethod("fallbackKeys");
        Set<String> seen = new HashSet<>();
        PrintStream out = new PrintStream(new FileOutputStream(args[0]), true, "UTF-8");
        for (int i = 1; i < args.length; i++) {
            try (JarFile jf = new JarFile(args[i])) {
                for (Enumeration<JarEntry> en = jf.entries(); en.hasMoreElements();) {
                    String n = en.nextElement().getName();
                    if (!n.endsWith(".class") || n.contains("$")) continue;
                    String cn = n.replace('/', '.').substring(0, n.length() - 6);
                    if (!cn.startsWith("org.apache.flink")) continue;
                    if (!(cn.endsWith("Options") || cn.contains("Option") || cn.endsWith("Config") || cn.endsWith("ConfigConstants"))) continue;
                    Class<?> c;
                    try { c = Class.forName(cn, true, DumpFlinkOptions.class.getClassLoader()); } catch (Throwable t) { continue; }
                    Field[] fs;
                    try { fs = c.getDeclaredFields(); } catch (Throwable t) { continue; }
                    for (Field f : fs) {
                        try {
                            if (!Modifier.isStatic(f.getModifiers()) || !co.isAssignableFrom(f.getType())) continue;
                            f.setAccessible(true);
                            Object opt = f.get(null);
                            if (opt == null) continue;
                            String key = (String) co.getMethod("key").invoke(opt);
                            boolean deprecatedField = f.isAnnotationPresent(Deprecated.class) || c.isAnnotationPresent(Deprecated.class);
                            Class<?> clazz = (Class<?>) clazzF.get(opt);
                            boolean isList = (Boolean) listF.get(opt);
                            Object def = co.getMethod("defaultValue").invoke(opt);
                            StringBuilder enums = new StringBuilder("[");
                            if (clazz != null && clazz.isEnum()) {
                                Object[] cs = clazz.getEnumConstants();
                                for (int k = 0; k < cs.length; k++) { if (k > 0) enums.append(","); enums.append(esc(((Enum<?>) cs[k]).name())); }
                            }
                            enums.append("]");
                            StringBuilder fbs = new StringBuilder("[");
                            boolean first = true;
                            for (Object fk : (Iterable<?>) fb.invoke(opt)) {
                                Method gk = fk.getClass().getMethod("getKey"); gk.setAccessible(true);
                                Method dep = fk.getClass().getMethod("isDeprecated"); dep.setAccessible(true);
                                if (!first) fbs.append(","); first = false;
                                fbs.append("{\"key\":").append(esc(gk.invoke(fk))).append(",\"deprecated\":").append(dep.invoke(fk)).append("}");
                            }
                            fbs.append("]");
                            String id = cn + "#" + key;
                            if (!seen.add(id)) continue;
                            out.println("{\"key\":" + esc(key) + ",\"class\":" + esc(cn) + ",\"field\":" + esc(f.getName())
                                + ",\"type\":" + esc(clazz == null ? null : clazz.getName()) + ",\"is_list\":" + isList
                                + ",\"enum\":" + enums + ",\"default\":" + esc(def) + ",\"fallback\":" + fbs
                                + ",\"deprecated_field\":" + deprecatedField + ",\"jar\":" + esc(new File(args[i]).getName()) + "}");
                        } catch (Throwable t) { /* skip */ }
                    }
                }
            }
        }
        out.close();
    }
}
