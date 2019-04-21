package homework.model;

import io.qala.datagen.RandomShortApi;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

import static io.qala.datagen.RandomShortApi.english;
import static io.qala.datagen.RandomShortApi.integer;

@Getter
@Setter
@Builder
public class City {
    private String name;
    private int population;
    private double area;
    private String country;
    private List<Transportation> transportation;

    public static City random() {
        return City.builder().name(english(1, 20))
                .country(english(4, 20))
                .population(integer(900, 26_320_000))
                .area(RandomShortApi.Double(0.44, 6.341))
                .transportation(Transportation.meansOfTransportation())
                .build();
    }

    public static List<City> randomCities(int numberToGenerate) {
        List<City> cities = new ArrayList<>();
        for (int i = 0; i < numberToGenerate; i++) {
            cities.add(random());
        }
        return cities;
    }
}


