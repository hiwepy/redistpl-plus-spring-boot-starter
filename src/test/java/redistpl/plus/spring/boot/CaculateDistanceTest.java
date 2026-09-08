package redistpl.plus.spring.boot;

import org.gavaghan.geodesy.Ellipsoid;
import org.gavaghan.geodesy.GlobalCoordinates;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.GeoTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CaculateDistanceTest {

	private final GeoTemplate geoTemplate = new GeoTemplate();

	@Test
	void calculatesDistanceWithoutRedisConnection() {
		double distance = geoTemplate.getDistance(39.95676, 116.401394, 36.63014, 114.499574);

		assertEquals(405404.17D, distance, 0.01D);
	}

	@Test
	void calculatesEllipsoidalDistanceWithoutRedisConnection() {
		GlobalCoordinates source = new GlobalCoordinates(39.95676, 116.401394);
		GlobalCoordinates target = new GlobalCoordinates(36.63014, 114.499574);

		assertEquals(405404.17D, geoTemplate.getDistance(source, target, Ellipsoid.Sphere), 0.01D);
		assertEquals(404982.96D, geoTemplate.getDistance(source, target, Ellipsoid.WGS84), 0.01D);
	}

    public static void main(String[] args){

		GeoTemplate geoTemplate = new GeoTemplate();
		double meterx = geoTemplate.getDistance(39.95676, 116.401394, 36.63014, 114.499574);
    	System.out.println("原始坐标系计算结果："+meterx + "米");

        GlobalCoordinates source = new GlobalCoordinates(39.95676, 116.401394);
        GlobalCoordinates target = new GlobalCoordinates(36.63014, 114.499574);

        double meter1 = geoTemplate.getDistance(source, target, Ellipsoid.Sphere);
        double meter2 = geoTemplate.getDistance(source, target, Ellipsoid.WGS84);

        System.out.println("Sphere坐标系计算结果："+meter1 + "米");
        System.out.println("WGS84坐标系计算结果："+meter2 + "米");

        // 坐标系计算结果：405858.3127090019米
        // Sphere坐标系计算结果：405404.16586678155米
        // WGS84坐标系计算结果：404982.95610057615米


    }


}
