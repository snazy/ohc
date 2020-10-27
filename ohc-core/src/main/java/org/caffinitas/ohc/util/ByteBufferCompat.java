package org.caffinitas.ohc.util;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import static org.objectweb.asm.Opcodes.*;

/**
 * Helper class that enables compilation with any Java version (e.g. 15) and being able to access
 * any {@link ByteBuffer} method.
 */
public final class ByteBufferCompat {
    private ByteBufferCompat() {
    }

    private static final ByteBufferAccess byteBufferAccess;

    static {
        try {
            Method m = ByteBuffer.class.getMethod("flip");

            int classFileVersion = V1_8;
            if (m.getReturnType() == ByteBuffer.class)
                classFileVersion = V11;

            // Generates a class named 'org.caffinitas.ohc.util.ByteBufferHelper' that
            // implements the ByteBufferAccess interface and acts as a bridge to
            // 'java.nio.ByteBuffer' using the correct signatures for BB.flip/position/clear/limit
            // methods, which got their signatures changed in Java 9.

            String owner = m.getReturnType().getName();
            String ownerSlash = owner.replace('.', '/');
            String ownerDesc = Type.getDescriptor(m.getReturnType());

            String classNameDots = "org.caffinitas.ohc.util.ByteBufferHelper";
            String classNameSlash = classNameDots.replace('.', '/');

            ClassWriter cw = new ClassWriter(0);
            cw.visit(classFileVersion,
                    ACC_PUBLIC + ACC_SUPER + ACC_FINAL,
                    classNameSlash,
                    null,
                    "java/lang/Object",
                    new String[]{ByteBufferAccess.class.getName().replace('.', '/')});
            cw.visitSource("ByteBufferHelper.java", null);

            String bbDesc = Type.getDescriptor(ByteBuffer.class);

            MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
            mv.visitVarInsn(ALOAD, 0);
            mv.visitMethodInsn(INVOKESPECIAL,
                    "java/lang/Object",
                    "<init>",
                    "()V",
                    false);
            mv.visitInsn(RETURN);
            mv.visitMaxs(1, 1);
            mv.visitEnd();

            mv = cw.visitMethod(ACC_PUBLIC + ACC_FINAL, "flip", "("+bbDesc+")V", null, null);
            mv.visitVarInsn(ALOAD, 1);
            mv.visitMethodInsn(INVOKEVIRTUAL, ownerSlash, "flip", "()"+ownerDesc, false);
            mv.visitInsn(POP);
            mv.visitInsn(RETURN);
            mv.visitMaxs(2, 2);
            mv.visitEnd();

            mv = cw.visitMethod(ACC_PUBLIC + ACC_FINAL, "clear", "("+bbDesc+")V", null, null);
            mv.visitVarInsn(ALOAD, 1);
            mv.visitMethodInsn(INVOKEVIRTUAL, ownerSlash, "clear", "()"+ownerDesc, false);
            mv.visitInsn(POP);
            mv.visitInsn(RETURN);
            mv.visitMaxs(2, 2);
            mv.visitEnd();

            mv = cw.visitMethod(ACC_PUBLIC + ACC_FINAL, "limit", "("+bbDesc+"I)V", null, null);
            mv.visitVarInsn(ALOAD, 1);
            mv.visitVarInsn(ILOAD, 2);
            mv.visitMethodInsn(INVOKEVIRTUAL, ownerSlash, "limit", "(I)"+ownerDesc, false);
            mv.visitInsn(POP);
            mv.visitInsn(RETURN);
            mv.visitMaxs(3, 3);
            mv.visitEnd();

            mv = cw.visitMethod(ACC_PUBLIC + ACC_FINAL, "position", "("+bbDesc+"I)V", null, null);
            mv.visitVarInsn(ALOAD, 1);
            mv.visitVarInsn(ILOAD, 2);
            mv.visitMethodInsn(INVOKEVIRTUAL, ownerSlash, "position", "(I)"+ownerDesc, false);
            mv.visitInsn(POP);
            mv.visitInsn(RETURN);
            mv.visitMaxs(3, 3);
            mv.visitEnd();

            byte[] classBytes = cw.toByteArray();

            ClassLoader cl = new ClassLoader(ByteBufferCompat.class.getClassLoader()) {
                @Override
                protected Class<?> findClass(String name) throws ClassNotFoundException {
                    if (classNameDots.equals(name))
                        return defineClass(name, classBytes, 0, classBytes.length);
                    throw new ClassNotFoundException(name);
                }
            };
            byteBufferAccess = (ByteBufferAccess) cl.loadClass(classNameDots).getDeclaredConstructor().newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public interface ByteBufferAccess {
        void flip(ByteBuffer byteBuffer);
        void clear(ByteBuffer byteBuffer);
        void limit(ByteBuffer byteBuffer, int limit);
        void position(ByteBuffer byteBuffer, int position);
    }

    public static void byteBufferFlip(ByteBuffer byteBuffer) {
        byteBufferAccess.flip(byteBuffer);
    }

    public static void byteBufferClear(ByteBuffer byteBuffer) {
        byteBufferAccess.clear(byteBuffer);
    }

    public static void byteBufferLimit(ByteBuffer byteBuffer, int limit) {
        byteBufferAccess.limit(byteBuffer, limit);
    }

    public static void byteBufferPosition(ByteBuffer byteBuffer, int position) {
        byteBufferAccess.position(byteBuffer, position);
    }
}
