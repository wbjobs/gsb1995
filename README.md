# gsb-eventstore

一个可长期演进、只依赖 JDK 8 标准库的文件事件存储。包名 `com.gsb.eventstore`，
源码在 `src/` 下，用 `javac` 直接编译，无 Maven/Gradle/JUnit/第三方依赖。

## 构建与测试

```sh
./run-tests.sh
```

脚本会用 `javac -source 1.8 -target 1.8` 编译 `src/` 与 `test/` 并运行
`com.gsb.eventstore.EventStoreTest`，全部断言通过时退出码为 0。

## 版本模型

系统里有两个互不混淆的"版本"：

- **事件版本（event version）**：stream 内的序号。`append(streamId, version, payload)`
  中的 `version` 就是它。同一 stream 内必须从 1 开始连续递增，不允许空洞、
  重复或乱序，违反时 `append` 抛 `IllegalStateException`。
- **模式版本（schema version）**：payload 形状的版本。每条事件落盘时记录写入
  当时的模式版本。写入时盖章的版本 = 从 v1 出发沿已注册 upcaster 链能连续到达
  的最高版本（注册了 `1->2`、`2->3` 则为 v3；只注册了 `2->3` 则链在 v1 处断开，
  新事件只能盖 v1）。读取时的目标版本 = 最高已注册版本（所有已注册
  `toVersion` 的最大值），链上有空洞时读取会显式失败而不是静默返回半成品。

存储格式：每个 stream 一个只追加的日志文件 `<dir>/<streamId>.events`，每行一条
事件：`E<eventVersion>;<schemaVersion>;<payload>`。payload 使用自实现的稳定文本
编码（`PayloadCodec`）：字符串为 UTF-8 的十六进制、Double 为 IEEE-754 原始位、
Map 按键排序输出，因此编码是确定性的，且任意字符（换行、制表符、分隔符本身）
都能原样往返。值类型只允许 String、Long、Boolean、Double、List、Map（递归校验，
Map 键必须是 String），其余类型在 `append` 时抛 `IllegalArgumentException`。

已写入的事件**不可变**：文件只追加，升级只发生在读取时的内存中，绝不回写。

## Upcaster 的注册与查找规则

- `registerUpcaster(fromVersion, toVersion, upcaster)` 只接受**单步**升级，
  即 `toVersion == fromVersion + 1`，否则抛 `IllegalArgumentException`；
  同一 `fromVersion` 重复注册抛 `IllegalStateException`。
- `read` 时对每条事件逐级升版：存储为 v1、当前版本为 v3 时，必须先走
  `v1 -> v2` 再走 `v2 -> v3`，**不允许跳级**。
- 某一级缺失时抛 `MissingUpcasterException`，异常消息中包含缺失的级别
  （如 `Missing upcaster: v1 -> v2 (stream 'orders', event #1)`），并通过
  `getFromVersion()` / `getToVersion()` 暴露。
- Upcaster 契约：输入是包含所有字段（含它不认识的字段）的完整 payload，
  实现应先复制再变换（`new LinkedHashMap<>(in)` 后增删改），从而把未知字段
  原样带下去；存储层自身从不裁剪任何字段。

## 向前 / 向后兼容的判定依据

- **向后兼容（新代码读旧数据）**：由 upcaster 链保证。只要从事件的存储版本到
  当前版本之间每一级单步 upcaster 都已注册，旧事件就能逐级升到最新形态；
  缺一级即显式失败（`MissingUpcasterException`），而不是静默产出错误数据。
- **向前兼容（旧代码读新数据）**：事件存储的模式版本高于当前注册链能达到的
  版本时，`read` 不报错、不降级，原样返回高版本 payload，新字段全部保留。
  因此"只注册到 v2 的图"读同一个文件：v1 事件读出为 v2 形态，v3 事件原样
  返回（含 v3 新字段），两种情况都不抛异常。
- **未知字段保留**：升级只替换 upcaster 显式变换的字段；未知字段经过任意多轮
  升级、再写回、再读出后仍深等于原值（有对应测试用例锁定该行为）。

## 目录结构

```
src/com/gsb/eventstore/EventStore.java                对外接口
src/com/gsb/eventstore/FileEventStore.java            文件实现（追加日志 + 读时升级）
src/com/gsb/eventstore/MissingUpcasterException.java  缺级异常
src/com/gsb/eventstore/PayloadCodec.java              稳定文本编解码（包私有）
test/com/gsb/eventstore/EventStoreTest.java           无依赖测试套件（main 运行）
run-tests.sh                                          编译并运行全部测试
```
