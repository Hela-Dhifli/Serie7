package exercice15;

import exercice15.model.Actor;
import exercice15.model.Movie;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;



public class ActorsAndMovies {

    public static void main(String[] args) {

        ActorsAndMovies actorsAndMovies = new ActorsAndMovies();
        Set<Movie> movies = actorsAndMovies.readMovies();
        
        // Question 1 
        
        System.out.println("\n******* Le nombre de films");
        
        System.out.println(movies.size());
        
        // Question 2
        
        	//Création d'un stream d'acteurs
        System.out.println("\n******* Le nombre d'acteurs");
        Stream<Actor> flatMap =
        		movies.stream()
        		.flatMap(m -> m.actors().stream());
        
        System.out.println(flatMap.count());
        
        	//Sans doublons
        System.out.println("\n******* Le nombre d'acteurs sans doublons");
        Stream<Actor> flatMap2 =
        		movies.stream()
        		.flatMap(actor -> actor.actors().stream())
        		.distinct();
        
        System.out.println(flatMap2.count());
        
        // Question 3
        
        System.out.println("\n******* Le nombre d'année");
        	//Stream d'année : correspondre un film à une année -> map()
        long anneesFilm = 
                movies.stream()
                .map(movie -> movie.releaseYear())
                .distinct()
                .count();
        
        System.out.println(anneesFilm);
        
        // Question 4
        
        System.out.println("\n******* Année du film le plus vieux");
        int anneeMin = 
        		movies.stream()
        		.map(movie -> movie.releaseYear())
        		.min(Integer::compare)
        		.orElseThrow();
        System.out.println(anneeMin);
        
        System.out.println("\n******* Année du film le plus récent");
        int anneeMax = 
        		movies.stream()
        		.mapToInt(movie -> movie.releaseYear())
        		.max()
        		.orElseThrow();
        System.out.println(anneeMax);

        // Question 5
        
        System.out.println("\n******* Année où le plus grand nombre de films est sorti");
        
        Entry<Integer, Long> maxFilmAnnee = movies.stream()
        		.collect(
        							Collectors.groupingBy(
        													e->e.releaseYear(),
        													Collectors.counting()
        												)
        							)
        		.entrySet().stream()
        		.max(Map.Entry.comparingByValue())
        		.orElseThrow();
     
        System.out.println(maxFilmAnnee);
        
        // Question 6
        
        System.out.println("\n******* Film avec le plus grand nombre d'acteur ");
        Movie maxActeurs = 
        		movies.stream()
        		.max(Comparator.comparing(movie -> movie.actors().size()))
        		.orElseThrow();
        System.out.println(
        		"Le titre du film est " + maxActeurs.title());
        System.out.println(
        		"Le nombre d'acteurs dans ce film est " + maxActeurs.actors().size());
        
        // Question 7
        
        System.out.println("\n******* L'acteur qui a joué dans le plus grand nombre de films");
        Stream<Actor> flatActeurs = 
        		movies.stream()
        		.flatMap(movie -> movie.actors().stream());
        Map<Actor, Long> collect = 
        		flatActeurs.collect(
        							Collectors.groupingBy(
        													Function.identity(),
        													Collectors.counting()
        												)
        							);
        Map.Entry<Actor, Long> acteur = 
        		collect.entrySet().stream()
        		.max(Map.Entry.comparingByValue())
        		.orElseThrow();
        System.out.println(acteur);
        
        // Question 8
        
        System.out.println("\n******* Question 7 avec uniquement un collector");
        Collector<Movie, Object, Entry<Actor, Long>> myCollector = Collectors.collectingAndThen(
			Collectors.flatMapping(
					movie -> movie.actors().stream(),
					Collectors.groupingBy(
											Function.identity(),
											Collectors.counting()
										)
									),
					m ->m.entrySet().stream()
					.max(Map.Entry.comparingByValue())
					.orElseThrow()
        		);
		Map.Entry<Actor, Long> acteur2 = 
        		movies.stream()
        		.collect(
	        				myCollector
        				);
        System.out.println(acteur2); 
        //on obtient le même affichage que la question 7
        
        
        System.out.println("\n******* L'acteur qui a joué dans le plus grand nombre de films en une année ");
        Map<Integer, Entry<Actor, Long>> acteurAnnee = movies.stream()
            	.collect(
            			Collectors.groupingBy(
            					y -> y.releaseYear(), 
            					myCollector
            			)
            	);
        Entry<Integer, Entry<Actor, Long>> acteur3 = 
        		acteurAnnee.entrySet().stream()
		        	.max(Comparator.comparing(e -> e.getValue().getValue()))
		        	.orElseThrow();
        System.out.println(acteur3);
        
        // Question 9-a
        Comparator<Actor> cmp=
				Comparator.comparing(Actor::lastName).thenComparing(Actor::firstName);
        
        // Question 9-b
        
        
       /*BiFunction<Stream<Actor>, Actor, Map.Entry<Map<Actor,List<Actor>, Actor>>  fct =
        		(s, act) -> Map.entry(
        							s.collect(
        										Collectors.groupingBy(Function.identity())
        									  )
        								,act);
        */
    }

    public Set<Movie> readMovies() {

        Function<String, Stream<Movie>> toMovie =
                line -> {
                    String[] elements = line.split("/");
                    String title = elements[0].substring(0, elements[0].lastIndexOf("(")).trim();
                    String releaseYear = elements[0].substring(elements[0].lastIndexOf("(") + 1, elements[0].lastIndexOf(")"));
                    if (releaseYear.contains(",")) {
                        // Movies with a coma in their title are discarded
                    	int indexOfComa = releaseYear.indexOf(",");
                    	releaseYear = releaseYear.substring(0,indexOfComa);
                        //return Stream.empty();
                    }
                    Movie movie = new Movie(title, Integer.valueOf(releaseYear));


                    for (int i = 1; i < elements.length; i++) {
                        String[] name = elements[i].split(", ");
                        String lastName = name[0].trim();
                        String firstName = "";
                        if (name.length > 1) {
                            firstName = name[1].trim();
                        }

                        Actor actor = new Actor(lastName, firstName);
                        movie.addActor(actor);
                    }
                    return Stream.of(movie);
                };

        try (FileInputStream fis = new FileInputStream("files/movies-mpaa.txt.gz");
             GZIPInputStream gzis = new GZIPInputStream(fis);
             InputStreamReader reader = new InputStreamReader(gzis);
             BufferedReader bufferedReader = new BufferedReader(reader);
             Stream<String> lines = bufferedReader.lines();
        ) {

            return lines.flatMap(toMovie).collect(Collectors.toSet());

        } catch (IOException e) {
            System.out.println("e.getMessage() = " + e.getMessage());
        }

        return Set.of();
    }
}
