package org.springframework.data.redis.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.*;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Tests for {@link TypeReferences}.
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 */
class TypeReferencesTest {

    @Test
    void mapType() {
        assertThat(TypeReferences.MAP_TYPE).isNotNull();
        assertThat(TypeReferences.MAP_TYPE).isInstanceOf(TypeReference.class);
    }

    @Test
    void listType() {
        assertThat(TypeReferences.LIST_TYPE).isNotNull();
        assertThat(TypeReferences.LIST_TYPE).isInstanceOf(TypeReference.class);
    }

    @Test
    void stringType() {
        assertThat(TypeReferences.STRING_TYPE).isNotNull();
    }

    @Test
    void integerType() {
        assertThat(TypeReferences.INTEGER_TYPE).isNotNull();
    }

    @Test
    void longType() {
        assertThat(TypeReferences.LONG_TYPE).isNotNull();
    }

    @Test
    void doubleType() {
        assertThat(TypeReferences.DOUBLE_TYPE).isNotNull();
    }

    @Test
    void getTypeFromClass() {
        TypeReference<String> ref = TypeReferences.getType(String.class);
        assertThat(ref).isNotNull();
    }

    @Test
    void getTypeFromClassCached() {
        TypeReference<String> ref1 = TypeReferences.getType(String.class);
        TypeReference<String> ref2 = TypeReferences.getType(String.class);
        assertThat(ref1).isSameAs(ref2);
    }

    @Test
    void getTypeFromType() {
        TypeReference<String> ref = TypeReferences.getType(String.class);
        assertThat(ref).isNotNull();
    }

    @Test
    void getListType() {
        TypeReference<List<String>> ref = TypeReferences.getListType(String.class);
        assertThat(ref).isNotNull();
    }

    @Test
    void getListTypeCached() {
        TypeReference<List<String>> ref1 = TypeReferences.getListType(String.class);
        TypeReference<List<String>> ref2 = TypeReferences.getListType(String.class);
        assertThat(ref1).isSameAs(ref2);
    }

    @Test
    void javaTypeReference() {
        TypeReferences.JavaTypeReference ref = new TypeReferences.JavaTypeReference(String.class);
        assertThat(ref.getType()).isEqualTo(String.class);
    }
}
