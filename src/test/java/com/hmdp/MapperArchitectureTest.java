package com.hmdp;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.hmdp.mapper.BlogMapper;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.junit.jupiter.api.Test;

/** 分层约束及 Mapper 参数绑定回归，防止重新把 SQL 写回业务代码。 */
class MapperArchitectureTest {

  @Test
  void productionDatabaseAccessUsesMappersAndControllersDependOnServices() throws Exception {
    Path sourceRoot = Paths.get("src/main/java");
    List<Path> sources;
    try (Stream<Path> files = Files.walk(sourceRoot)) {
      sources =
          files.filter(path -> path.toString().endsWith(".java")).collect(Collectors.toList());
    }
    for (Path source : sources) {
      String text = new String(Files.readAllBytes(source), StandardCharsets.UTF_8);
      assertFalse(text.contains("JdbcTemplate"), source.toString());
      assertFalse(
          text.matches("(?s).*private\\s+(?:final\\s+)?(?:org|com)\\..*"), source.toString());
      if (!source.getFileName().toString().endsWith("Controller.java")) {
        continue;
      }
      String className =
          sourceRoot.relativize(source).toString().replace('/', '.').replace(".java", "");
      Class<?> controller = Class.forName(className, false, getClass().getClassLoader());
      for (Field field : controller.getDeclaredFields()) {
        if (field.isSynthetic() || Modifier.isStatic(field.getModifiers())) {
          continue;
        }
        assertTrue(
            field.getType().isInterface()
                && field.getType().getPackage().getName().equals("com.hmdp.service"),
            className + "." + field.getName());
      }
    }
  }

  @Test
  void mapperXmlStatementsBindDeclaredParametersInsteadOfConcatenatingSql() throws Exception {
    MybatisConfiguration configuration = mapperConfiguration();
    try (Stream<Path> files = Files.list(Paths.get("src/main/resources/mapper"))) {
      for (Path file :
          files.filter(path -> path.toString().endsWith(".xml")).collect(Collectors.toList())) {
        String name = file.getFileName().toString().replace(".xml", "");
        Class<?> mapper = Class.forName("com.hmdp.mapper." + name);
        for (Method method : mapper.getDeclaredMethods()) {
          Map<String, Object> arguments = new HashMap<>();
          for (Parameter parameter : method.getParameters()) {
            Param param = parameter.getAnnotation(Param.class);
            assertNotNull(param, mapper.getName() + "." + method.getName());
            Class<?> type = parameter.getType();
            Object value =
                type == String.class
                    ? "sample"
                    : type == List.class
                        ? Collections.singletonList(1L)
                        : type == Timestamp.class
                            ? new Timestamp(0)
                            : type == Integer.class ? 1 : 1L;
            arguments.put(param.value(), value);
          }
          MappedStatement statement =
              configuration.getMappedStatement(mapper.getName() + "." + method.getName());
          BoundSql boundSql = statement.getBoundSql(arguments);
          assertFalse(boundSql.getSql().contains("${"), statement.getId());
          boundSql
              .getParameterMappings()
              .forEach(
                  mapping ->
                      assertTrue(
                          arguments.containsKey(mapping.getProperty())
                              || boundSql.hasAdditionalParameter(mapping.getProperty()),
                          statement.getId() + ": " + mapping.getProperty()));
        }
      }
    }
  }

  @Test
  void emptyLikeBatchCannotProduceInvalidInClause() throws Exception {
    MybatisConfiguration configuration = mapperConfiguration();
    Map<String, Object> arguments = new HashMap<>();
    arguments.put("userId", 1L);
    arguments.put("blogIds", Collections.emptyList());
    String sql =
        configuration
            .getMappedStatement(BlogMapper.class.getName() + ".selectLikedBlogIds")
            .getBoundSql(arguments)
            .getSql();
    assertTrue(sql.contains("AND 1 = 0"));
    assertFalse(sql.contains("IN ()"));
  }

  private MybatisConfiguration mapperConfiguration() throws Exception {
    MybatisConfiguration configuration = new MybatisConfiguration();
    try (Stream<Path> files = Files.list(Paths.get("src/main/resources/mapper"))) {
      for (Path file :
          files.filter(path -> path.toString().endsWith(".xml")).collect(Collectors.toList())) {
        try (InputStream input = Files.newInputStream(file)) {
          new XMLMapperBuilder(
                  input, configuration, file.toString(), configuration.getSqlFragments())
              .parse();
        }
      }
    }
    return configuration;
  }
}
