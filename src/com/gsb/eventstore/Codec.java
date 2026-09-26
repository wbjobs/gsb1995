package com.gsb.eventstore;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Stable, line-safe text encoding for event payloads. Only the six supported
 * value types can be encoded: String, Long, Boolean, Double, List, Map.
 *
 * Grammar (recursive and self-delimiting, so no escaping is ever needed):
 *   value   := string | long | boolean | double | array | map
 *   string  := 'S' base64(utf8 bytes) ';'
 *   long    := 'L' decimal digits ';'
 *   boolean := 'T' | 'F'
 *   double  := 'D' hex(doubleToLongBits) ';'
 *   array   := 'A' count ':' value*count
 *   map     := 'M' count ':' (value value)*count   // key is always a string
 *
 * The encoding never contains whitespace or line breaks, so one encoded
 * payload always fits on a single line of the event file. Doubles are stored
 * as their raw bit pattern, so the round trip is lossless (NaN, -0.0, ...).
 */
final class Codec {

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private Codec() {
    }

    static String encodeMap(Map<String, Object> map) {
        StringBuilder out = new StringBuilder();
        encodeValue(map, out);
        return out.toString();
    }

    static Map<String, Object> decodeMap(String text) {
        Cursor cursor = new Cursor(text);
        Object value = decodeValue(cursor);
        if (!(value instanceof Map)) {
            throw new IllegalStateException("Corrupt event payload: top-level value is not a map");
        }
        if (!cursor.atEnd()) {
            throw new IllegalStateException(
                    "Corrupt event payload: trailing characters at offset " + cursor.position());
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) value;
        return map;
    }

    private static void encodeValue(Object value, StringBuilder out) {
        if (value instanceof String) {
            out.append('S');
            out.append(Base64.getEncoder().encodeToString(((String) value).getBytes(UTF_8)));
            out.append(';');
        } else if (value instanceof Long) {
            out.append('L').append(value.toString()).append(';');
        } else if (value instanceof Boolean) {
            out.append(((Boolean) value).booleanValue() ? 'T' : 'F');
        } else if (value instanceof Double) {
            out.append('D');
            out.append(Long.toHexString(Double.doubleToLongBits(((Double) value).doubleValue())));
            out.append(';');
        } else if (value instanceof List) {
            List<?> list = (List<?>) value;
            out.append('A').append(list.size()).append(':');
            for (Object element : list) {
                encodeValue(element, out);
            }
        } else if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            out.append('M').append(map.size()).append(':');
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                encodeValue(entry.getKey(), out);
                encodeValue(entry.getValue(), out);
            }
        } else {
            throw new IllegalArgumentException("Unsupported value type: "
                    + (value == null ? "null" : value.getClass().getName()));
        }
    }

    private static Object decodeValue(Cursor cursor) {
        char tag = cursor.next();
        switch (tag) {
            case 'S':
                return new String(Base64.getDecoder().decode(cursor.readUntil(';')), UTF_8);
            case 'L':
                return Long.valueOf(Long.parseLong(cursor.readUntil(';')));
            case 'T':
                return Boolean.TRUE;
            case 'F':
                return Boolean.FALSE;
            case 'D':
                return Double.valueOf(
                        Double.longBitsToDouble(Long.parseUnsignedLong(cursor.readUntil(';'), 16)));
            case 'A': {
                int size = cursor.readIntUntil(':');
                List<Object> list = new ArrayList<Object>(size);
                for (int i = 0; i < size; i++) {
                    list.add(decodeValue(cursor));
                }
                return list;
            }
            case 'M': {
                int size = cursor.readIntUntil(':');
                Map<String, Object> map = new LinkedHashMap<String, Object>();
                for (int i = 0; i < size; i++) {
                    Object key = decodeValue(cursor);
                    if (!(key instanceof String)) {
                        throw new IllegalStateException("Corrupt event payload: map key is not a string");
                    }
                    map.put((String) key, decodeValue(cursor));
                }
                return map;
            }
            default:
                throw new IllegalStateException("Corrupt event payload: unknown type tag '" + tag
                        + "' at offset " + (cursor.position() - 1));
        }
    }

    private static final class Cursor {
        private final String text;
        private int pos;

        Cursor(String text) {
            this.text = text;
        }

        boolean atEnd() {
            return pos == text.length();
        }

        int position() {
            return pos;
        }

        char next() {
            if (pos >= text.length()) {
                throw new IllegalStateException("Corrupt event payload: unexpected end of data");
            }
            return text.charAt(pos++);
        }

        String readUntil(char delimiter) {
            int end = text.indexOf(delimiter, pos);
            if (end < 0) {
                throw new IllegalStateException("Corrupt event payload: missing '" + delimiter + "'");
            }
            String token = text.substring(pos, end);
            pos = end + 1;
            return token;
        }

        int readIntUntil(char delimiter) {
            String token = readUntil(delimiter);
            try {
                return Integer.parseInt(token);
            } catch (NumberFormatException e) {
                throw new IllegalStateException("Corrupt event payload: bad number '" + token + "'");
            }
        }
    }
}
