package json_replace;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Hayden Fields
 */
public class JsonReplace
{
    
    public static class NodeDepth
    {
        JsonNode node;
        int depth;

        public NodeDepth (JsonNode node, int depth)
        {
            this.node = node;
            this.depth = depth;
        }

    }

    public static Map<String, String> queryMock = new HashMap<>();
    public static void main (String[] args) throws IOException, IllegalAccessException
    {
        Pattern queryPattern = Pattern.compile("^\\{DynamicSql:(?<sql>[^}]*)\\}$");
        
        queryMock.put("select from cbk_cases where issuer_reference_number = '1234'", "4321");
        
        ObjectMapper mapper = new ObjectMapper();
        
        Stack<NodeDepth> queue = new Stack<>();
        
        queue.push(new NodeDepth(mapper.readTree(new File("src/json_replace/input")), 0));
        
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        boolean isRoot = true;
        /*
        If we see an object without name then am root
        
        */
        JsonNode before = queue.peek().node;
        System.out.println(before);
        while (!queue.isEmpty())
        {
            NodeDepth payload = queue.pop();
            JsonNode node = payload.node;
            final int depth = payload.depth;
            
            System.out.printf(depth==0?"":String.format("%%%ds", 4 * depth), "");
            System.out.printf("%s\n", node.getNodeType());
            
            if (node.isArray())
            {
                node.elements().forEachRemaining(n ->
                {
                    queue.push(new NodeDepth(n, depth + 1));
                });
            }
            else if (node.isObject())
            {
                ObjectNode object = (ObjectNode)node;
                
                node.fields().forEachRemaining(entry -> {
                    String fieldName = entry.getKey();
                    JsonNode child = entry.getValue();
                     
                    queue.push(new NodeDepth(child, depth + 1));
                });
            }
            else if (node.isValueNode())
            {
                if (node.isTextual())
                {
                    TextNode textNode = (TextNode)node;
                    String text = textNode.textValue();
                    Matcher m = queryPattern.matcher(text);
                    // if find then do query and make replacement
                    if (m.find())
                    {
                        try
                        {
                            Field value = textNode.getClass().getDeclaredField("_value");
                            value.setAccessible(true);
                            String sql = m.group("sql");
                            List<Map<String, String>> resultSet = doQuery(sql);
                            if (resultSet.isEmpty())
                                throw new IllegalArgumentException(String.format("Sql statement %s returns nothing", sql));
                            else if (resultSet.size() == 1 && resultSet.get(0).size() == 1)
                                value.set(textNode, resultSet.get(0).values().iterator().next());
                            else // we got more than 1 so we create an array of objects
                            {
                            }
                        } catch (NoSuchFieldException | SecurityException ex)
                        {
                            ex.printStackTrace(System.out);
                        }
                    }
                }
                System.out.printf(depth==0?"":String.format("%%%ds", 4 * depth), "");
                System.out.printf("  %s\n", node.asText());
            }
        }
        System.out.println(before);
    }
    
    public static List<Map<String, String>> doQuery(String sql)
    {
        try(Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:54321/postgres"))
        {
            PreparedStatement ps = connection.prepareStatement(sql);
            
            ResultSet rs = ps.executeQuery();
        } catch (SQLException ex)
        {
            ex.printStackTrace(System.out);
        }
        // have to handle numeric, text, boolean
        return null;
    }
}
