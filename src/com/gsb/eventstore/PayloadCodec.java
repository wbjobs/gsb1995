package com.gsb.eventstore;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Stable, self-describing text encoding for event payloads. No JSON library,
 * no Java serialization. Grammar (recursive descent):
 *
 * <pre>
 *   value  := string | long | bool | double | list | map
 *   string := 'S' hex(utf8 bytes) ';'
 *   long   := 'L' ['-'] digits ';'
 *   bool   := 'B' ('0' | '1') ';'
 *   double := 'D' hex(raw IEEE-754 bits) ';'
 *   list   := 'A' value* 'Z'
 *   map    := 'M' (string value)* 'Z'      (keys sorted for canonical output)
 * </pre>
 *
 * Strings are hex-encoded UTF-8, so any character (including newlines, tabs and
 * the delimiter characters themselves) round-trips safely and one event always
 * occupies exactly one line in the log file. Doubles are stored as raw bits so
 * NaN, -0.0 and infinities round-trip exactly. Map keys are sorted on encode so
 * the same logical payload always produces the same bytes.
 */
final class PayloadCodec {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private PayloadCodec() {
    }

    static String encodeMap(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder();
        encodeValue(map, sb);
        return sb.toString();
    }

    static Map<String, Object> decodeMap(String text) {
        int[] pos = new int[1];
        Object value = parseValue(text, pos);
        if (pos[0] != text.length()) {
            throw corrupt("trailing data at offset " + pos[0]);
        }
        return castMap(value);
    }

    @SuppressWarnings("unchecked")
    private static void encodeValue(Object value, StringBuilder sb) {
        if (value instanceof String) {
            sb.append('S');
            appendHex(((String) value).getBytes(StandardCharsets.UTF_8), sb);
            sb.append(';');
        } else if (value instanceof Long) {
            sb.append('L').append(value.toString()).append(';');
        } else if (value instanceof Boolean) {
            sb.append('B').append(((Boolean) value).booleanValue() ? '1' : '0').append(';');
        } else if (value instanceof Double) {
            sb.append('D').append(Long.toHexString(
                    Double.doubleToRawLongBits(((Double) value).doubleValue()))).append(';');
        } else if (value instanceof List) {
            sb.append('A');
            for (Object item : (List<Object>) value) {
                encodeValue(item, sb);
            }
            sb.append('Z');
        } else if (value instanceof Map) {
            sb.append('M');
            Map<String, Object> sorted = new TreeMap<String, Object>((Map<String, Object>) value);
            for (Map.Entry<String, Object> entry : sorted.entrySet()) {
                encodeValue(entry.getKey(), sb);
                encodeValue(entry.getValue(), sb);
            }
            sb.append('Z');
        } else {
            throw new IllegalArgumentException("unsupported payload value type: "
                    + (value == null ? "null" : value.getClass().getName()));
        }
    }

    private static Object parseValue(String s, int[] pos) {
        if (pos[0] >= s.length()) {
            throw corrupt("unexpected end of input");
        }
        char type = s.charAt(pos[0]++);
        switch (type) {
            case 'S': {
                int start = pos[0];
                while (pos[0] < s.length() && s.charAt(pos[0]) != ';') {
                    pos[0]++;
                }
                require(pos[0] < s.length(), "unterminated string");
                String text = new String(fromHex(s.substring(start, pos[0])), StandardCharsets.UTF_8);
                pos[0]++;
                return text;
            }
            case 'L': {
                int start = pos[0];
                while (pos[0] < s.length() && s.charAt(pos[0]) != ';') {
                    pos[0]++;
                }
                require(pos[0] < s.length(), "unterminated long");
                try {
                    long v = Long.parseLong(s.substring(start, pos[0]));
                    pos[0]++;
                    return Long.valueOf(v);
                } catch (NumberFormatException e) {
                    throw corrupt("bad long literal");
                }
            }
            case 'B': {
                require(pos[0] + 1 < s.length(), "unterminated boolean");
                char c = s.charAt(pos[0]++);
                require(c == '0' || c == '1', "bad boolean literal");
                require(s.charAt(pos[0]++) == ';', "boolean missing terminator");
                return Boolean.valueOf(c == '1');
            }
            case 'D': {
                int start = pos[0];
                while (pos[0] < s.length() && s.charAt(pos[0]) != ';') {
                    pos[0]++;
                }
                require(pos[0] < s.length(), "unterminated double");
                try {
                    long bits = Long.parseLong(s.substring(start, pos[0]), 16);
                    pos[0]++;
                    return Double.valueOf(Double.longBitsToDouble(bits));
                } catch (NumberFormatException e) {
                    throw corrupt("bad double literal");
                }
            }
            case 'A': {
                List<Object> list = new ArrayList<Object>();
                while (true) {
                    require(pos[0] < s.length(), "unterminated list");
                    if (s.charAt(pos[0]) == 'Z') {
                        pos[0]++;
                        return list;
                    }
                    list.add(parseValue(s, pos));
                }
            }
            case 'M': {
                Map<String, Object> map = new LinkedHashMap<String, Object>();
                while (true) {
                    require(pos[0] < s.length(), "unterminated map");
                    if (s.charAt(pos[0]) == 'Z') {
                        pos[0]++;
                        return map;
                    }
                    Object key = parseValue(s, pos);
                    if (!(key instanceof String)) {
                        throw corrupt("map key is not a string");
                    }
                    map.put((String) key, parseValue(s, pos));
                }
            }
            default:
                throw corrupt("unknown type tag '" + type + "' at offset " + (pos[0] - 1));
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Object value) {
        return (Map<String, Object>) value;
    }

    private static void appendHex(byte[] bytes, StringBuilder sb) {
        for (byte b : bytes) {
            sb.append(HEX[(b >> 4) & 0xF]).append(HEX[b & 0xF]);
        }
    }

    private static byte[] fromHex(String s) {
        if ((s.length() & 1) != 0) {
            throw corrupt("odd-length hex string");
        }
        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) ((hexValue(s.charAt(2 * i)) << 4) | hexValue(s.charAt(2 * i + 1)));
        }
        return out;
    }

    private static int hexValue(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return c - 'a' + 10;
        }
        throw corrupt("bad hex digit '" + c + "'");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw corrupt(message);
        }
    }

    private static IllegalStateException corrupt(String message) {
        return new IllegalStateException("corrupt payload encoding: " + message);
    }
}
