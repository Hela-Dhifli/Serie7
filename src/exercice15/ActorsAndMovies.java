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
import java.util.function.BiFunction;
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
        Stream<Actor> nbActeurs =
        		movies.stream()
        		.flatMap(m -> m.actors().stream());
        
        System.out.println(nbActeurs.count());
        
        	//Sans doublons
        System.out.println("\n******* Le nombre d'acteurs sans doublons");
        Stream<Actor> nbActeurs2 =
        		movies.stream()
        		.flatMap(actor -> actor.actors().stream())
        		.distinct();
        
        System.out.println(nbActeurs2.count());
        
        // Question 3
        
        System.out.println("\n******* Le nombre d'année référencées dans le fichier");
        	//Stream d'année : correspondre un film à une année -> map()
        long nbAnnée = 
                movies.stream()
                .map(movie -> movie.releaseYear())
                .distinct()
                .count();
        
        System.out.println(nbAnnée);
        
        // Question 4
        
        System.out.println("\n******* Année du film le plus vieux\n");
        int anneeMin = 
        		movies.stream()
        		.map(movie -> movie.releaseYear())
        		.min(Integer::compare)
        		.orElseThrow();
        System.out.println(anneeMin);
        
        System.out.println("\n******* Année du film le plus récent\n");
        int anneeMax = 
        		movies.stream()
        		.mapToInt(movie -> movie.releaseYear())
        		.max()
        		.orElseThrow();
        System.out.println(anneeMax);

        // Question 5
        
        System.out.println("\n******* Année où le plus grand nombre de films est sorti\n");
        
        Entry<Integer, Long> anneeMaxFilm = movies.stream()
        		.collect(
        							Collectors.groupingBy(
        													e->e.releaseYear(),
        													Collectors.counting()
        												)
        							)
        		.entrySet().stream()
        		.max(Map.Entry.comparingByValue())
        		.orElseThrow();
     
        System.out.println("L'année est " +  anneeMaxFilm.getKey());
        System.out.println("Le nombre de films est " +  anneeMaxFilm.getValue());
        
        // Question 6
        
        System.out.println("\n******* Film avec le plus grand nombre d'acteur\n ");
        Movie maxActeurs = 
        		movies.stream()
        		.max(Comparator.comparing(movie -> movie.actors().size()))
        		.orElseThrow();
        System.out.println(
        		"Le titre du film est " + maxActeurs.title());
        System.out.println(
        		"Le nombre d'acteurs dans ce film est " + maxActeurs.actors().size());
        
        // Question 7
        
        System.out.println("\n******* L'acteur qui a joué dans le plus grand nombre de films\n");
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
        System.out.println(acteur.getKey() + "\n Il a joué dans " + acteur.getValue() +" films");
        
        // Question 8
        
        System.out.println("\n******* Question 7 avec uniquement un collecto\n");
        Collector<Movie, Object, Entry<Actor, Long>> myCollector = 
        		Collectors.collectingAndThen(
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
        System.out.println(acteur2.getKey() + "\n Il a joué dans " + acteur.getValue() +" films");
        		//on obtient le même affichage que la question 7
        
        System.out.println("\n******* L'acteur qui a joué dans le plus grand nombre de films en une année \n");
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
        System.out.println(
			        		acteur3.getValue().getKey() + "\nIl a joué " 
			                + acteur3.getValue().getValue() + " films en " 
			        		+ acteur3.getKey()
		        		);
        
        // Question 9-a
        Comparator<Actor> comparatorActor=
				Comparator.comparing(Actor::lastName).thenComparing(Actor::firstName);
        
        // Question 9-b
        		
        BiFunction<Stream<Actor>, Actor, Stream<Map.Entry<Actor, Actor>>>  steamPaireActeurs =
        		(s, act) -> s.filter( 
        								act2 -> comparatorActor.compare(act, act2) < 0)
        								.map(sec -> Map.entry(sec, act)
        							);
        
		
        // Question 9-c
        
        Function<Movie, Stream<Actor>> streamActeur =
        		m -> m.actors().stream();
        		
        // Question 9-d
        
        BiFunction<Movie, Actor, Stream<Map.Entry<Actor, Actor>>> streamPaireActeurFilm = 
        		(film, act) -> steamPaireActeurs
        						.apply (streamActeur.apply(film), act);
        		
        // Question 9-e
        
        Function<Movie, Stream<Map.Entry<Actor, Actor>>>  streamPaireActeurFilmFunction = 
        		film -> film.actors().stream()
        				.flatMap(
        						act -> streamPaireActeurFilm.apply(film, act)
        						);
        
        
        // Question 9-f
        System.out.println("\n******* Le nombre de paires d'acteurs dans le fichier");
        		  
        long nbPaireActeurFichier = 
        		movies.stream()
        		.flatMap(t -> streamPaireActeurFilmFunction
        				.apply(t)
        				)
        		.count();
        System.out.println(nbPaireActeurFichier);
        
        // Question 9-g
        System.out.println("\n******* Les deux acteurs qui ont joué le plus souvent ensemble");
        
        Entry<Entry<Actor, Actor>, Long> paireActeursPlusEnsemble = 
        		movies.stream()
        		.flatMap(t -> streamPaireActeurFilmFunction
        				.apply(t)
        				)
        		.collect(
        							Collectors.groupingBy(
        													Function.identity(),
        													Collectors.counting()
        												)
        							)
        		.entrySet().stream()
        		.max(Map.Entry.comparingByValue())
        		.orElseThrow();
        
        System.out.println(paireActeursPlusEnsemble.getKey().getKey() + "\n"
        		+ paireActeursPlusEnsemble.getKey().getValue() + "\nIls ont joué " 
        		+paireActeursPlusEnsemble.getValue() + " fois ensemble");
        
        //Question 10
        System.out.println("\n******* Les deux acteurs qui ont joué le plus ensemble durant une année ");
        	//Collector
        Collector<Movie, Movie, Entry<Entry<Actor,Actor>, Long>> myCollector2 = 
        		Collectors.collectingAndThen(
		    			Collectors.flatMapping(
		    					movie -> streamPaireActeurFilmFunction.apply(movie),
		    					Collectors.groupingBy(
		    											Function.identity(),
		    											Collectors.counting()
		    										)
		    									),
		    					map ->map.entrySet().stream()
		    					.max(Map.Entry.comparingByValue())
		    					.orElseThrow()
            		);
        
        Entry<Integer,  Entry<Entry<Actor,Actor>, Long>> paireActeurAnnee=
        		movies.stream()
        		.collect(
            			Collectors.groupingBy(
            					y -> y.releaseYear(),
            					myCollector2
            	
            			)
            	)
        		.entrySet().stream()
		        .max(Comparator.comparing(e -> e.getValue().getValue()))
		        .orElseThrow();
        
        System.out.println(
        		paireActeurAnnee.getValue().getKey().getKey()
        		+ "\n" + paireActeurAnnee.getValue().getKey().getValue()
        		+ "\nIls ont joué le plus ensemble en " + paireActeurAnnee.getKey());
        
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
