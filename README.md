# com.gsb.eventstore — 可长期演进的事件存储

纯 JDK 8 标准库实现（无 Maven/Gradle/JUnit/第三方依赖），源码在 `src/` 下，
用 `javac` 直接编译。对外入口是 `com.gsb.eventstore.EventStore`：

```java
EventStore store = new EventStore("/path/to/data-dir");
store.registerUpcaster(1, 2, event -> { /* ... */ return upgraded; });
store.append("orders", 1, payload);            // version 从 1 开始连续递增
List<Map<String, Object>> events = store.read("orders");
```

运行验收测试：

```bash
./run-tests.sh
```

脚本会用 `javac -source 8 -target 8` 编译 `src/` 与 `tests/` 并运行全部断言；
在 WSL 里如果找不到 Linux 版 `javac`，会自动回退到 Windows 侧的 JDK。

## 版本模型

- 每个 stream 对应一个 append-only 文件 `<streamId>.events`，每行一个事件：
  `<version> <编码后的负载>`。
- 同一 stream 内版本号从 1 开始、每次 +1 连续递增；`append` 强制校验，
  出现空洞或重复版本号会抛 `IllegalStateException`。
- 事件的存储版本号同时就是它的 schema 版本号。
- 已写入的行永不修改、永不删除：升级只发生在读取时的内存中，
  存储文件保持不变（原始事件不可变）。

## 文本编码

不依赖任何 JSON 库，使用自定义的递归自定界文本编码（`Codec`，包私有）：

```
value   := string | long | boolean | double | array | map
string  := 'S' base64(utf8) ';'
long    := 'L' 十进制 ';'
boolean := 'T' | 'F'
double  := 'D' doubleToLongBits 的十六进制 ';'
array   := 'A' 元素数 ':' value*N
map     := 'M' 键值对数 ':' (key value)*N
```

- 只允许六种值类型：String、Long、Boolean、Double、List、Map（递归校验，
  其他类型如 Integer 会在 `append` 时被拒绝并提示改用 Long）。
- 字符串按 UTF-8 字节 Base64 编码，因此任意字符（换行、分隔符、中文、emoji）
  都无需转义且单行安全；Double 按原始位模式存储，往返无损（含 -0.0、NaN）。
- 编码结果是确定的：同样的负载永远编码成同样的字节序列，读回后与原负载深等于。

## Upcaster 的注册与查找规则

- `registerUpcaster(fromVersion, toVersion, upcaster)` 只允许
  `toVersion = fromVersion + 1`，即逐级注册，不允许跳级注册。
- 读取时的目标版本 = 已注册 upcaster 中最高的 `toVersion`（一个都没注册时为 0，
  即不做任何升级）。
- 对每条事件：若 `存储版本 < 目标版本`，从存储版本开始**逐级**应用
  `v -> v+1` 的 upcaster，直到目标版本。例如 v1 事件在注册了
  v1->v2、v2->v3 后，先经 v1->v2 再经 v2->v3 升到 v3。
- 缺某一级时抛 `MissingUpcasterException`，异常消息明确指出缺的是哪一级
  （如 `Missing upcaster from version 1 to version 2`）。
- 若 `存储版本 >= 目标版本`，事件原样返回，不做任何处理。
- 升级是读取路径上的纯函数链：upcaster 收到的是从文件解码出的全新 Map，
  返回新 Map；存储文件从不被重写。

## 向前 / 向后兼容的判定依据

- **向后兼容（旧代码读新数据）**：只注册到 v2 的 reader 读同一个文件时，
  目标版本是 2；存储为 v3 的事件因 `存储版本 >= 目标版本` 被原样透传，
  更高版本的新字段完整保留，不报错。
- **向前兼容（新代码读旧数据）**：存储为 v1 的事件在读取时逐级升到当前
  最高版本；只要 upcaster 采用"复制后修改已知字段"的写法（测试中的
  upcaster 即此风格），未知字段会随 Map 一起被带下去，升级再写回后仍完全保留。
- 兼容性成立的根本依据：存储不可变（append-only）+ 编码无损（六种类型
  精确往返）+ 升级为内存中的纯函数链。因此同一文件可以被任意版本的
  upcaster 图安全读取，新字段不会因读取/升级而丢失。

## 目录结构

```
src/com/gsb/eventstore/EventStore.java               对外 API
src/com/gsb/eventstore/Codec.java                    稳定文本编码（包私有）
src/com/gsb/eventstore/MissingUpcasterException.java 缺级异常
tests/com/gsb/eventstore/EventStoreTest.java         验收测试（无框架，main 驱动）
run-tests.sh                                         编译并运行全部测试
```
