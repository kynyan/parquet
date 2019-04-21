package homework.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import javax.xml.crypto.dsig.TransformService;

import java.util.ArrayList;
import java.util.List;

import static io.qala.datagen.RandomShortApi.*;

@Getter @Setter
@Builder
public class Transportation {
    private String type;
    private String name;

    public static Transportation random() {
        return Transportation.builder()
                .type(sample("bus", "urban rail", "train", "airports", "cycling"))
                .name(english(5, 20)).build();
    }

    public static List<Transportation> meansOfTransportation() {
        List<Transportation> transportation = new ArrayList<>();
        for (int i = 0; i < integer(1, 5); i++) {
            transportation.add(Transportation.random());
        }
        return transportation;
    }
}
