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
package com.facebook.presto.rewriter.optplus;

import com.facebook.presto.client.Column;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.UuidType;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;

import java.util.List;
import java.util.stream.Collectors;

public class RewriterUtils
{
    private RewriterUtils() {}

    private static final String varcharType = VarcharType.VARCHAR.getTypeSignature().toString();
    private static final String charType = CharType.createCharType(10).getTypeSignature().toString().replace("(10)", "");
    private static final String varbinaryType = VarbinaryType.VARBINARY.getTypeSignature().toString();
    private static final String dateType = DateType.DATE.getTypeSignature().toString();
    private static final String timeType = TimeType.TIME.getTypeSignature().toString();
    private static final String timeStampType = TimestampType.TIMESTAMP.getTypeSignature().toString();
    private static final String uuidType = UuidType.UUID.getTypeSignature().toString();

    public static String generateValuesQuery(List<List<Object>> rows, List<Column> columnNames)
    {
        rows = rows.stream().filter(row -> !row.isEmpty()).collect(Collectors.toList());
        if (rows.isEmpty()) {
            return "SELECT 0 LIMIT 0";
        }
        StringBuilder sb = new StringBuilder(" SELECT * FROM (VALUES ");
        for (int j = 0; j < rows.size(); j++) {
            sb.append("(");
            List<Object> currentRow = rows.get(j);
            if (currentRow.isEmpty()) {
                sb.append("''"); // Handle the empty row case.
            }
            for (int i = 0; i < currentRow.size(); i++) {
                Object currentObj = currentRow.get(i);
                if (columnNames.size() == currentRow.size()) {
                    String columnType = columnNames.get(i).getType();
                    sb.append(applyTypeInformation(columnType, currentObj));
                }
                else { // if column type info not available assume strings.
                    sb.append(applyTypeInformation(varcharType, currentObj));
                }
                if (i < currentRow.size() - 1) {
                    sb.append(", ");
                }
            }
            if (j < rows.size() - 1) {
                sb.append("), ");
            }
        }
        sb.append("))");
        if (columnNames.size() == rows.get(0).size()) {
            sb.append(" AS t (\"");
            for (int i = 0; i < columnNames.size(); i++) {
                sb.append(columnNames.get(i).getName());
                if (i < columnNames.size() - 1) {
                    sb.append("\", \"");
                }
            }
            sb.append("\")");
        }
        sb.trimToSize();
        return sb.toString();
    }

    // TODO : do we need to support structured types?
    private static String applyTypeInformation(String columnType, Object currentObj)
    {
        StringBuilder sb = new StringBuilder();
        if (columnType.startsWith(varcharType) || columnType.startsWith(charType) || columnType.startsWith(varbinaryType)) {
            sb.append(processStringsWithEscapeChars(currentObj));
        }
        else if (columnType.equals(dateType)) {
            sb.append("DATE ").append("'").append(currentObj).append("'");
        }
        else if (columnType.equals(timeType)) {
            sb.append("TIME ").append("'").append(currentObj).append("'");
        }
        else if (columnType.equals(timeStampType)) {
            sb.append("TIMESTAMP ").append("'").append(currentObj).append("'");
        }
        else if (columnType.equals(uuidType)) {
            sb.append("UUID ").append("'").append(currentObj).append("'");
        }
        else { // for all numeric types don't apply quotes.
            sb.append(currentObj);
        }
        sb.trimToSize();
        return sb.toString();
    }

    private static String processStringsWithEscapeChars(Object currentObj)
    {
        StringBuilder sb = new StringBuilder();
        String currentObjString = currentObj + ""; // if null, then it will be converted to "null" string value.
        if (currentObjString.contains("'")) {
            // escape single quotes.
            currentObjString = currentObjString.replaceAll("'", "''");
        }
        // apply quotes
        sb.append("'").append(currentObjString).append("'");

        return sb.toString();
    }
}
