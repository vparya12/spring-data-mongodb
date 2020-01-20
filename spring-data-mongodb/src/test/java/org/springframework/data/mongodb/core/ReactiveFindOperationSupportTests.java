/*
 * Copyright 2017-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.annotation.Id;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeospatialIndex;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.test.util.MongoTestUtils;

/**
 * Integration tests for {@link ReactiveFindOperationSupport}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class ReactiveFindOperationSupportTests {

	private static final String STAR_WARS = "star-wars";
	MongoTemplate blocking;
	ReactiveMongoTemplate template;

	Person han;
	Person luke;

	@Before
	public void setUp() {

		blocking = new MongoTemplate(
				new SimpleMongoClientDatabaseFactory(MongoTestUtils.client(), "ExecutableFindOperationSupportTests"));
		recreateCollection(STAR_WARS, false);

		insertObjects();

		template = new ReactiveMongoTemplate(MongoTestUtils.reactiveClient(), "ExecutableFindOperationSupportTests");
	}

	void insertObjects() {

		han = new Person();
		han.firstname = "han";
		han.lastname = "solo";
		han.id = "id-1";

		luke = new Person();
		luke.firstname = "luke";
		luke.lastname = "skywalker";
		luke.id = "id-2";

		blocking.save(han);
		blocking.save(luke);
	}

	void recreateCollection(String collectionName, boolean capped) {

		blocking.dropCollection(STAR_WARS);

		CollectionOptions options = CollectionOptions.empty();
		if (capped) {
			options = options.capped().size(1024 * 1024);
		}

		blocking.createCollection(STAR_WARS, options);
	}

	@Test // DATAMONGO-1719
	public void domainTypeIsRequired() {
		assertThatIllegalArgumentException().isThrownBy(() -> template.query(null));
	}

	@Test // DATAMONGO-1719
	public void returnTypeIsRequiredOnSet() {
		assertThatIllegalArgumentException().isThrownBy(() -> template.query(Person.class).as(null));
	}

	@Test // DATAMONGO-1719
	public void collectionIsRequiredOnSet() {
		assertThatIllegalArgumentException().isThrownBy(() -> template.query(Person.class).inCollection(null));
	}

	@Test // DATAMONGO-1719
	public void findAll() {

		template.query(Person.class).all().collectList().as(StepVerifier::create).consumeNextWith(actual -> {
			assertThat(actual).containsExactlyInAnyOrder(han, luke);
		}).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllWithCollection() {
		template.query(Human.class).inCollection(STAR_WARS).all().as(StepVerifier::create).expectNextCount(2)
				.verifyComplete();
	}

	@Test // DATAMONGO-2323
	public void findAllAsDocumentDocument() {
		template.query(Document.class).inCollection(STAR_WARS).all().as(StepVerifier::create).expectNextCount(2)
				.verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllWithProjection() {

		template.query(Person.class).as(Jedi.class).all().map(it -> it.getClass().getName()).as(StepVerifier::create) //
				.expectNext(Jedi.class.getName(), Jedi.class.getName()) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllBy() {

		template.query(Person.class).matching(query(where("firstname").is("luke"))).all().as(StepVerifier::create) //
				.expectNext(luke) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllByWithCollectionUsingMappingInformation() {

		template.query(Jedi.class).inCollection(STAR_WARS).matching(query(where("name").is("luke"))).all()
				.as(StepVerifier::create).consumeNextWith(it -> assertThat(it).isInstanceOf(Jedi.class)) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllByWithCollection() {

		template.query(Human.class).inCollection(STAR_WARS).matching(query(where("firstname").is("luke"))).all()
				.as(StepVerifier::create).expectNextCount(1) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllByWithProjection() {

		template.query(Person.class).as(Jedi.class).matching(query(where("firstname").is("luke"))).all()
				.as(StepVerifier::create).consumeNextWith(it -> assertThat(it).isInstanceOf(Jedi.class)) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllByWithClosedInterfaceProjection() {

		template.query(Person.class).as(PersonProjection.class).matching(query(where("firstname").is("luke"))).all()
				.as(StepVerifier::create).consumeNextWith(it -> {

					assertThat(it).isInstanceOf(PersonProjection.class);
					assertThat(it.getFirstname()).isEqualTo("luke");
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllByWithOpenInterfaceProjection() {

		template.query(Person.class).as(PersonSpELProjection.class).matching(query(where("firstname").is("luke"))).all()
				.as(StepVerifier::create).consumeNextWith(it -> {

					assertThat(it).isInstanceOf(PersonSpELProjection.class);
					assertThat(it.getName()).isEqualTo("luke");
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findBy() {

		template.query(Person.class).matching(query(where("firstname").is("luke"))).one().as(StepVerifier::create)
				.expectNext(luke) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findByNoMatch() {

		template.query(Person.class).matching(query(where("firstname").is("spock"))).one().as(StepVerifier::create)
				.verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findByTooManyResults() {

		template.query(Person.class).matching(query(where("firstname").in("han", "luke"))).one().as(StepVerifier::create)
				.expectError(IncorrectResultSizeDataAccessException.class) //
				.verify();
	}

	@Test // DATAMONGO-1719
	public void findAllNearBy() {

		blocking.indexOps(Planet.class).ensureIndex(
				new GeospatialIndex("coordinates").typed(GeoSpatialIndexType.GEO_2DSPHERE).named("planet-coordinate-idx"));

		Planet alderan = new Planet("alderan", new Point(-73.9836, 40.7538));
		Planet dantooine = new Planet("dantooine", new Point(-73.9928, 40.7193));

		blocking.save(alderan);
		blocking.save(dantooine);

		template.query(Planet.class).near(NearQuery.near(-73.9667, 40.78).spherical(true)).all().as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual.getDistance()).isNotNull();
				}) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllNearByWithCollectionAndProjection() {

		blocking.indexOps(Planet.class).ensureIndex(
				new GeospatialIndex("coordinates").typed(GeoSpatialIndexType.GEO_2DSPHERE).named("planet-coordinate-idx"));

		Planet alderan = new Planet("alderan", new Point(-73.9836, 40.7538));
		Planet dantooine = new Planet("dantooine", new Point(-73.9928, 40.7193));

		blocking.save(alderan);
		blocking.save(dantooine);

		template.query(Object.class).inCollection(STAR_WARS).as(Human.class)
				.near(NearQuery.near(-73.9667, 40.78).spherical(true)).all().as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual.getDistance()).isNotNull();
					assertThat(actual.getContent()).isInstanceOf(Human.class);
					assertThat(actual.getContent().getId()).isEqualTo("alderan");
				}) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllNearByReturningGeoResultContentAsClosedInterfaceProjection() {

		blocking.indexOps(Planet.class).ensureIndex(
				new GeospatialIndex("coordinates").typed(GeoSpatialIndexType.GEO_2DSPHERE).named("planet-coordinate-idx"));

		Planet alderan = new Planet("alderan", new Point(-73.9836, 40.7538));
		Planet dantooine = new Planet("dantooine", new Point(-73.9928, 40.7193));

		blocking.save(alderan);
		blocking.save(dantooine);

		template.query(Planet.class).as(PlanetProjection.class).near(NearQuery.near(-73.9667, 40.78).spherical(true)).all()
				.as(StepVerifier::create).consumeNextWith(it -> {

					assertThat(it.getDistance()).isNotNull();
					assertThat(it.getContent()).isInstanceOf(PlanetProjection.class);
					assertThat(it.getContent().getName()).isEqualTo("alderan");
				}) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void findAllNearByReturningGeoResultContentAsOpenInterfaceProjection() {

		blocking.indexOps(Planet.class).ensureIndex(
				new GeospatialIndex("coordinates").typed(GeoSpatialIndexType.GEO_2DSPHERE).named("planet-coordinate-idx"));

		Planet alderan = new Planet("alderan", new Point(-73.9836, 40.7538));
		Planet dantooine = new Planet("dantooine", new Point(-73.9928, 40.7193));

		blocking.save(alderan);
		blocking.save(dantooine);

		template.query(Planet.class).as(PlanetSpELProjection.class).near(NearQuery.near(-73.9667, 40.78).spherical(true))
				.all().as(StepVerifier::create).consumeNextWith(it -> {

					assertThat(it.getDistance()).isNotNull();
					assertThat(it.getContent()).isInstanceOf(PlanetSpELProjection.class);
					assertThat(it.getContent().getId()).isEqualTo("alderan");
				}) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2080
	public void tail() throws InterruptedException {

		recreateCollection(STAR_WARS, true);
		insertObjects();

		BlockingQueue<Person> collector = new LinkedBlockingQueue<>();
		Flux<Person> tail = template.query(Person.class)
				.matching(query(new Criteria().orOperator(where("firstname").is("chewbacca"), where("firstname").is("luke"))))
				.tail().doOnNext(collector::add);

		Disposable subscription = tail.subscribe();

		assertThat(collector.poll(1, TimeUnit.SECONDS)).isEqualTo(luke);
		assertThat(collector).isEmpty();

		Person chewbacca = new Person();
		chewbacca.firstname = "chewbacca";
		chewbacca.lastname = "chewie";
		chewbacca.id = "id-3";

		blocking.save(chewbacca);

		assertThat(collector.poll(1, TimeUnit.SECONDS)).isEqualTo(chewbacca);

		subscription.dispose();
	}

	@Test // DATAMONGO-2080
	public void tailWithProjection() {

		recreateCollection(STAR_WARS, true);
		insertObjects();

		template.query(Person.class).as(Jedi.class).matching(query(where("firstname").is("luke"))).tail()
				.as(StepVerifier::create) //
				.consumeNextWith(it -> assertThat(it).isInstanceOf(Jedi.class)) //
				.thenCancel() //
				.verify();
	}

	@Test // DATAMONGO-2080
	public void tailWithClosedInterfaceProjection() {

		recreateCollection(STAR_WARS, true);
		insertObjects();

		template.query(Person.class).as(PersonProjection.class).matching(query(where("firstname").is("luke"))).tail()
				.as(StepVerifier::create) //
				.consumeNextWith(it -> {

					assertThat(it).isInstanceOf(PersonProjection.class);
					assertThat(it.getFirstname()).isEqualTo("luke");
				}) //
				.thenCancel() //
				.verify();
	}

	@Test // DATAMONGO-2080
	public void tailWithOpenInterfaceProjection() {

		recreateCollection(STAR_WARS, true);
		insertObjects();

		template.query(Person.class).as(PersonSpELProjection.class).matching(query(where("firstname").is("luke"))).tail()
				.as(StepVerifier::create) //
				.consumeNextWith(it -> {

					assertThat(it).isInstanceOf(PersonSpELProjection.class);
					assertThat(it.getName()).isEqualTo("luke");
				}) //
				.thenCancel() //
				.verify();
	}

	@Test // DATAMONGO-1719
	public void firstShouldReturnFirstEntryInCollection() {
		template.query(Person.class).first().as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void countShouldReturnNrOfElementsInCollectionWhenNoQueryPresent() {
		template.query(Person.class).count().as(StepVerifier::create).expectNext(2L).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void countShouldReturnNrOfElementsMatchingQuery() {

		template.query(Person.class).matching(query(where("firstname").is(luke.getFirstname()))).count()
				.as(StepVerifier::create).expectNext(1L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void existsShouldReturnTrueIfAtLeastOneElementExistsInCollection() {
		template.query(Person.class).exists().as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void existsShouldReturnFalseIfNoElementExistsInCollection() {

		blocking.remove(new BasicQuery("{}"), STAR_WARS);

		template.query(Person.class).exists().as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void existsShouldReturnTrueIfAtLeastOneElementMatchesQuery() {

		template.query(Person.class).matching(query(where("firstname").is(luke.getFirstname()))).exists()
				.as(StepVerifier::create).expectNext(true) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void existsShouldReturnFalseWhenNoElementMatchesQuery() {

		template.query(Person.class).matching(query(where("firstname").is("spock"))).exists().as(StepVerifier::create)
				.expectNext(false) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsEmptyListIfNoMatchFound() {

		template.query(Person.class).distinct("actually-not-property-in-use").as(String.class).all()
				.as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsSimpleFieldValuesCorrectlyForCollectionHavingReturnTypeSpecifiedThatCanBeConvertedDirectlyByACodec() {

		Person anakin = new Person();
		anakin.firstname = "anakin";
		anakin.lastname = luke.lastname;

		blocking.save(anakin);

		template.query(Person.class).distinct("lastname").as(String.class).all().as(StepVerifier::create)
				.assertNext(in("solo", "skywalker")).assertNext(in("solo", "skywalker")) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsSimpleFieldValuesCorrectly() {

		Person anakin = new Person();
		anakin.firstname = "anakin";
		anakin.ability = "dark-lord";

		Person padme = new Person();
		padme.firstname = "padme";
		padme.ability = 42L;

		Person jaja = new Person();
		jaja.firstname = "jaja";
		jaja.ability = new Date();

		blocking.save(anakin);
		blocking.save(padme);
		blocking.save(jaja);

		Consumer<Object> containedInAbilities = in(anakin.ability, padme.ability, jaja.ability);

		template.query(Person.class).distinct("ability").all().as(StepVerifier::create) //
				.assertNext(containedInAbilities) //
				.assertNext(containedInAbilities) //
				.assertNext(containedInAbilities) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsComplexValuesCorrectly() {

		Sith sith = new Sith();
		sith.rank = "lord";

		Person anakin = new Person();
		anakin.firstname = "anakin";
		anakin.ability = sith;

		blocking.save(anakin);

		template.query(Person.class).distinct("ability").all().as(StepVerifier::create) //
				.expectNext(anakin.ability) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsComplexValuesCorrectlyHavingReturnTypeSpecified() {

		Sith sith = new Sith();
		sith.rank = "lord";

		Person anakin = new Person();
		anakin.firstname = "anakin";
		anakin.ability = sith;

		blocking.save(anakin);

		template.query(Person.class).distinct("ability").as(Sith.class).all().as(StepVerifier::create) //
				.expectNext(sith) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsComplexValuesCorrectlyReturnTypeDocumentSpecified() {

		Sith sith = new Sith();
		sith.rank = "lord";

		Person anakin = new Person();
		anakin.firstname = "anakin";
		anakin.ability = sith;

		blocking.save(anakin);

		template.query(Person.class).distinct("ability").as(Document.class).all().as(StepVerifier::create)
				.expectNext(new Document("rank", "lord").append("_class", Sith.class.getName())) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctMapsFieldNameCorrectly() {

		template.query(Jedi.class).inCollection(STAR_WARS).distinct("name").as(String.class).all().as(StepVerifier::create)
				.assertNext(in("han", "luke")).assertNext(in("han", "luke")) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsRawValuesIfReturnTypeIsBsonValue() {

		Consumer<BsonValue> inValues = in(new BsonString("solo"), new BsonString("skywalker"));
		template.query(Person.class).distinct("lastname").as(BsonValue.class).all().as(StepVerifier::create)
				.assertNext(inValues) //
				.assertNext(inValues) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsValuesMappedToTheirJavaTypeEvenWhenNotExplicitlyDefinedByTheDomainType() {

		blocking.save(new Document("darth", "vader"), STAR_WARS);

		template.query(Person.class).distinct("darth").all().as(StepVerifier::create) //
				.expectNext("vader") //
				.verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsMappedDomainTypeForProjections() {

		luke.father = new Person();
		luke.father.firstname = "anakin";

		blocking.save(luke);

		template.query(Person.class).distinct("father").as(Jedi.class).all().as(StepVerifier::create)
				.expectNext(new Jedi("anakin")) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctAlllowsQueryUsingObjectSourceType() {

		luke.father = new Person();
		luke.father.firstname = "anakin";

		blocking.save(luke);

		template.query(Object.class).inCollection(STAR_WARS).distinct("father").as(Jedi.class).all()
				.as(StepVerifier::create).expectNext(new Jedi("anakin")) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctReturnsMappedDomainTypeExtractedFromPropertyWhenNoExplicitTypePresent() {

		luke.father = new Person();
		luke.father.firstname = "anakin";

		blocking.save(luke);

		Person expected = new Person();
		expected.firstname = luke.father.firstname;

		template.query(Person.class).distinct("father").all().as(StepVerifier::create) //
				.expectNext(expected) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1761
	public void distinctThrowsExceptionWhenExplicitMappingTypeCannotBeApplied() {

		template.query(Person.class).distinct("firstname").as(Long.class).all().as(StepVerifier::create)
				.expectError(InvalidDataAccessApiUsageException.class) //
				.verify();
	}

	interface Contact {}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document(collection = STAR_WARS)
	static class Person implements Contact {

		@Id String id;
		String firstname;
		String lastname;
		Object ability;
		Person father;
	}

	interface PersonProjection {
		String getFirstname();
	}

	public interface PersonSpELProjection {

		@Value("#{target.firstname}")
		String getName();
	}

	@Data
	static class Human {
		@Id String id;
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	static class Jedi {

		@Field("firstname") String name;
	}

	@Data
	static class Sith {

		String rank;
	}

	@Data
	@AllArgsConstructor
	@org.springframework.data.mongodb.core.mapping.Document(collection = STAR_WARS)
	static class Planet {

		@Id String name;
		Point coordinates;
	}

	interface PlanetProjection {
		String getName();
	}

	interface PlanetSpELProjection {

		@Value("#{target.name}")
		String getId();
	}

	static <T> Consumer<T> in(T... values) {
		return (val) -> {
			assertThat(values).contains(val);
		};
	}
}
