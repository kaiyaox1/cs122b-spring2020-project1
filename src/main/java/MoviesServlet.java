import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import javax.annotation.Resource;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

@WebServlet(name = "MoviesServlet",urlPatterns = "/api/movies")
public class MoviesServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Resource(name ="jdbc/moviedc")
    private DataSource dataSource;

    /**
     * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException{
        response.setContentType("application/json");

        PrintWriter out = response.getWriter();

        try {
            Connection dbcon = dataSource.getConnection();

            Statement statement = dbcon.createStatement();

            String query = "SELECT movies.id,movies.title,year,director,substring_index(group_concat(DISTINCT stars.name SEPARATOR ','),',',3) as allstars,substring_index(group_concat(DISTINCT genres.name SEPARATOR ','), ',', 3) as allgenres,rating FROM movies,stars,stars_in_movies,genres,genres_in_movies,ratings WHERE movies.id = stars_in_movies.movieId AND stars.id = stars_in_movies.starId AND genres.id = genres_in_movies.genreId AND movies.id = genres_in_movies.movieId AND movies.id = ratings.movieId GROUP BY movies.id,movies.title,movies.year,movies.director,ratings.rating ORDER BY ratings.rating DESC limit 0,20;";

            ResultSet rs = statement.executeQuery(query);

            JsonArray jsonArray = new JsonArray();

            // Iterate through each row of rs
            while (rs.next()) {
                String id = rs.getString("id");
                String title = rs.getString("title");
                String year = rs.getString("year");
                String director = rs.getString("director");
                String allstars = rs.getString("allstars");
                String allgenres = rs.getString("allgenres");
                String rating = rs.getString("rating");

                // Create a JsonObject based on the data we retrieve from rs
                JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty("id", id);
                jsonObject.addProperty("title", title);
                jsonObject.addProperty("year", year);
                jsonObject.addProperty("director", director);
                jsonObject.addProperty("allstars", allstars);
                jsonObject.addProperty("allgenres", allgenres);
                jsonObject.addProperty("rating", rating);

                jsonArray.add(jsonObject);
            }

            // write JSON string to output
            out.write(jsonArray.toString());
            // set response status to 200 (OK)
            response.setStatus(200);

            rs.close();
            statement.close();
            dbcon.close();
        }catch (Exception e) {

            // write error message JSON object to output
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("errorMessage", e.getMessage());
            out.write(jsonObject.toString());

            // set reponse status to 500 (Internal Server Error)
            response.setStatus(500);

        }
        out.close();
    }
}
