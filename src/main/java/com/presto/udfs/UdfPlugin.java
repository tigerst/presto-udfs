/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.presto.udfs;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.presto.json.CustomJsonPathType;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class UdfPlugin implements Plugin {

    private List<Class<?>> getFunctionClasses() throws IOException {
        List<Class<?>> classes = Lists.newArrayList();
        String classResource = this.getClass().getName().replace(".", "/") + ".class";
        String jarURLFile = Thread.currentThread().getContextClassLoader().getResource(classResource).getFile();
        int jarEnd = jarURLFile.indexOf('!');
        // This is in URL format, convert once more to get actual file location
        String jarLocation = jarURLFile.substring(0, jarEnd);
        System.out.println("jarLocation=====" + jarLocation);
        jarLocation = new URL(jarLocation).getFile();

        ZipInputStream zip = new ZipInputStream(new FileInputStream(jarLocation));
        for (ZipEntry entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
            if (entry.getName().endsWith(".class") && !entry.isDirectory()) {
                // This still has .class at the end
                String className = entry.getName().replace("/", ".");
                // remvove .class from end
                className = className.substring(0, className.length() - 6);
                try {
                    //System.out.println("className===" +className);
                    if(className.startsWith("com.presto.udfs.scalar")&& !className.contains("$")&& !className.equals("UdfPlugin")){
                        classes.add(Class.forName(className));
                    }

                } catch (ClassNotFoundException e) {
                    System.out.println("Could not load class {}, Exception: {} "+  className);
                    e.printStackTrace();
                }
            }
        }
        return classes;
    }

    @Override
    public Set<Class<?>> getFunctions() {
        try {
            List<Class<?>> classes = getFunctionClasses();
            Set<Class<?>> set = Sets.newHashSet();
            for (Class<?> clazz : classes) {
                //System.out.println(Thread.currentThread().getContextClassLoader()+"Adding: " + clazz);
                if (clazz.getName().startsWith("com.presto.udfs.scalar")&& !clazz.getName().contains("$") && !clazz.getName().equals("UdfPlugin")) {
                    set.add(clazz);
                }
            }
            return ImmutableSet.<Class<?>>builder().addAll(set).build();
        } catch (IOException e) {
            System.out.println("Could not load classes from jar file: {} ");
            e.printStackTrace();
            return ImmutableSet.of();
        }

    }


    @Override
    public Iterable<Type> getTypes() {
        List<Type> types = new ArrayList<Type>();
        types.add(CustomJsonPathType.JSON_PATH);
        return types;
    }

    public static void main(String[] args){
        try {
            new UdfPlugin().getFunctionClasses();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
