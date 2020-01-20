/*
 * Copyright 2014-2020 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.ExecutableFindOperation.ExecutableFind;
import org.springframework.data.mongodb.core.ExecutableFindOperation.FindWithQuery;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.Person;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.Meta;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.mongodb.client.result.DeleteResult;

/**
 * Unit tests for {@link AbstractMongoQuery}.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class AbstractMongoQueryUnitTests {

	@Mock MongoOperations mongoOperationsMock;
	@Mock ExecutableFind<?> executableFind;
	@Mock FindWithQuery<?> withQueryMock;
	@Mock BasicMongoPersistentEntity<?> persitentEntityMock;
	@Mock MongoMappingContext mappingContextMock;
	@Mock DeleteResult deleteResultMock;

	@Before
	public void setUp() {

		doReturn("persons").when(persitentEntityMock).getCollection();
		doReturn(persitentEntityMock).when(mappingContextMock).getPersistentEntity(Mockito.any(Class.class));
		doReturn(persitentEntityMock).when(mappingContextMock).getRequiredPersistentEntity(Mockito.any(Class.class));
		doReturn(Person.class).when(persitentEntityMock).getType();

		DbRefResolver dbRefResolver = new DefaultDbRefResolver(mock(MongoDatabaseFactory.class));
		MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, mappingContextMock);
		converter.afterPropertiesSet();

		doReturn(converter).when(mongoOperationsMock).getConverter();
		doReturn(executableFind).when(mongoOperationsMock).query(any());
		doReturn(withQueryMock).when(executableFind).as(any());
		doReturn(withQueryMock).when(withQueryMock).matching(any());

		when(mongoOperationsMock.remove(any(), any(), anyString())).thenReturn(deleteResultMock);
	}

	@Test // DATAMONGO-566
	public void testDeleteExecutionCallsRemoveCorrectly() {

		createQueryForMethod("deletePersonByLastname", String.class).setDeleteQuery(true).execute(new Object[] { "booh" });

		verify(mongoOperationsMock, times(1)).remove(any(), eq(Person.class), eq("persons"));
		verify(mongoOperationsMock, times(0)).find(any(), any(), any());
	}

	@Test // DATAMONGO-566, DATAMONGO-1040
	public void testDeleteExecutionLoadsListOfRemovedDocumentsWhenReturnTypeIsCollectionLike() {

		createQueryForMethod("deleteByLastname", String.class).setDeleteQuery(true).execute(new Object[] { "booh" });

		verify(mongoOperationsMock, times(1)).findAllAndRemove(any(), eq(Person.class), eq("persons"));
	}

	@Test // DATAMONGO-566
	public void testDeleteExecutionReturnsZeroWhenWriteResultIsNull() {

		MongoQueryFake query = createQueryForMethod("deletePersonByLastname", String.class);
		query.setDeleteQuery(true);

		assertThat(query.execute(new Object[] { "fake" })).isEqualTo(0L);
	}

	@Test // DATAMONGO-566, DATAMONGO-978
	public void testDeleteExecutionReturnsNrDocumentsDeletedFromWriteResult() {

		when(deleteResultMock.getDeletedCount()).thenReturn(100L);
		when(deleteResultMock.wasAcknowledged()).thenReturn(true);

		MongoQueryFake query = createQueryForMethod("deletePersonByLastname", String.class);
		query.setDeleteQuery(true);

		assertThat(query.execute(new Object[] { "fake" })).isEqualTo(100L);
		verify(mongoOperationsMock, times(1)).remove(any(), eq(Person.class), eq("persons"));
	}

	@Test // DATAMONGO-957
	public void metadataShouldNotBeAddedToQueryWhenNotPresent() {

		MongoQueryFake query = createQueryForMethod("findByFirstname", String.class);
		query.execute(new Object[] { "fake" });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(executableFind).as(Person.class);
		verify(withQueryMock).matching(captor.capture());

		assertThat(captor.getValue().getMeta().getComment()).isNull();
		;
	}

	@Test // DATAMONGO-957
	public void metadataShouldBeAddedToQueryCorrectly() {

		MongoQueryFake query = createQueryForMethod("findByFirstname", String.class, Pageable.class);
		query.execute(new Object[] { "fake", PageRequest.of(0, 10) });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(executableFind).as(Person.class);
		verify(withQueryMock).matching(captor.capture());

		assertThat(captor.getValue().getMeta().getComment()).isEqualTo("comment");
	}

	@Test // DATAMONGO-957
	public void metadataShouldBeAddedToCountQueryCorrectly() {

		MongoQueryFake query = createQueryForMethod("findByFirstname", String.class, Pageable.class);
		query.execute(new Object[] { "fake", PageRequest.of(1, 10) });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(executableFind).as(Person.class);
		verify(withQueryMock, atLeast(1)).matching(captor.capture());

		assertThat(captor.getValue().getMeta().getComment()).isEqualTo("comment");
	}

	@Test // DATAMONGO-957, DATAMONGO-1783
	public void metadataShouldBeAddedToStringBasedQueryCorrectly() {

		MongoQueryFake query = createQueryForMethod("findByAnnotatedQuery", String.class, Pageable.class);
		query.execute(new Object[] { "fake", PageRequest.of(0, 10) });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(executableFind).as(Person.class);
		verify(withQueryMock).matching(captor.capture());

		assertThat(captor.getValue().getMeta().getComment()).isEqualTo("comment");
	}

	@Test // DATAMONGO-1057
	public void slicedExecutionShouldRetainNrOfElementsToSkip() {

		MongoQueryFake query = createQueryForMethod("findByLastname", String.class, Pageable.class);
		Pageable page1 = PageRequest.of(0, 10);
		Pageable page2 = page1.next();

		query.execute(new Object[] { "fake", page1 });
		query.execute(new Object[] { "fake", page2 });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(executableFind, times(2)).as(Person.class);
		verify(withQueryMock, times(2)).matching(captor.capture());

		assertThat(captor.getAllValues().get(0).getSkip()).isZero();
		assertThat(captor.getAllValues().get(1).getSkip()).isEqualTo(10);
	}

	@Test // DATAMONGO-1057
	public void slicedExecutionShouldIncrementLimitByOne() {

		MongoQueryFake query = createQueryForMethod("findByLastname", String.class, Pageable.class);
		Pageable page1 = PageRequest.of(0, 10);
		Pageable page2 = page1.next();

		query.execute(new Object[] { "fake", page1 });
		query.execute(new Object[] { "fake", page2 });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(executableFind, times(2)).as(Person.class);
		verify(withQueryMock, times(2)).matching(captor.capture());

		assertThat(captor.getAllValues().get(0).getLimit()).isEqualTo(11);
		assertThat(captor.getAllValues().get(1).getLimit()).isEqualTo(11);
	}

	@Test // DATAMONGO-1057
	public void slicedExecutionShouldRetainSort() {

		MongoQueryFake query = createQueryForMethod("findByLastname", String.class, Pageable.class);
		Pageable page1 = PageRequest.of(0, 10, Sort.Direction.DESC, "bar");
		Pageable page2 = page1.next();

		query.execute(new Object[] { "fake", page1 });
		query.execute(new Object[] { "fake", page2 });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(executableFind, times(2)).as(Person.class);
		verify(withQueryMock, times(2)).matching(captor.capture());

		Document expectedSortObject = new Document().append("bar", -1);
		assertThat(captor.getAllValues().get(0).getSortObject()).isEqualTo(expectedSortObject);
		assertThat(captor.getAllValues().get(1).getSortObject()).isEqualTo(expectedSortObject);
	}

	@Test // DATAMONGO-1080
	public void doesNotTryToPostProcessQueryResultIntoWrapperType() {

		Person reference = new Person();

		doReturn(reference).when(withQueryMock).oneValue();

		AbstractMongoQuery query = createQueryForMethod("findByLastname", String.class);

		assertThat(query.execute(new Object[] { "lastname" })).isEqualTo(reference);
	}

	@Test // DATAMONGO-1865
	public void limitingSingleEntityQueryCallsFirst() {

		Person reference = new Person();

		doReturn(reference).when(withQueryMock).firstValue();

		AbstractMongoQuery query = createQueryForMethod("findFirstByLastname", String.class).setLimitingQuery(true);

		assertThat(query.execute(new Object[] { "lastname" })).isEqualTo(reference);
	}

	@Test // DATAMONGO-1872
	public void doesNotFixCollectionOnPreparation() {

		AbstractMongoQuery query = createQueryForMethod(DynamicallyMappedRepository.class, "findBy");

		query.execute(new Object[0]);

		verify(executableFind, never()).inCollection(anyString());
		verify(executableFind).as(DynamicallyMapped.class);
	}

	@Test // DATAMONGO-1979
	public void usesAnnotatedSortWhenPresent() {

		createQueryForMethod("findByAge", Integer.class) //
				.execute(new Object[] { 1000 });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getSortObject()).isEqualTo(new Document("age", 1));
	}

	@Test // DATAMONGO-1979
	public void usesExplicitSortOverridesAnnotatedSortWhenPresent() {

		createQueryForMethod("findByAge", Integer.class, Sort.class) //
				.execute(new Object[] { 1000, Sort.by(Direction.DESC, "age") });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getSortObject()).isEqualTo(new Document("age", -1));
	}

	@Test // DATAMONGO-1854
	public void shouldApplyStaticAnnotatedCollation() {

		createQueryForMethod("findWithCollationUsingSpimpleStringValueByFirstName", String.class) //
				.execute(new Object[] { "dalinar" });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // DATAMONGO-1854
	public void shouldApplyStaticAnnotatedCollationAsDocument() {

		createQueryForMethod("findWithCollationUsingDocumentByFirstName", String.class) //
				.execute(new Object[] { "dalinar" });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // DATAMONGO-1854
	public void shouldApplyDynamicAnnotatedCollationAsString() {

		createQueryForMethod("findWithCollationUsingPlaceholderByFirstName", String.class, Object.class) //
				.execute(new Object[] { "dalinar", "en_US" });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // DATAMONGO-1854
	public void shouldApplyDynamicAnnotatedCollationAsDocument() {

		createQueryForMethod("findWithCollationUsingPlaceholderByFirstName", String.class, Object.class) //
				.execute(new Object[] { "dalinar", new Document("locale", "en_US") });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // DATAMONGO-1854
	public void shouldApplyDynamicAnnotatedCollationAsLocale() {

		createQueryForMethod("findWithCollationUsingPlaceholderByFirstName", String.class, Object.class) //
				.execute(new Object[] { "dalinar", Locale.US });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1854
	public void shouldThrowExceptionOnNonParsableCollation() {

		createQueryForMethod("findWithCollationUsingPlaceholderByFirstName", String.class, Object.class) //
				.execute(new Object[] { "dalinar", 100 });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // DATAMONGO-1854
	public void shouldApplyDynamicAnnotatedCollationIn() {

		createQueryForMethod("findWithCollationUsingPlaceholderInDocumentByFirstName", String.class, String.class) //
				.execute(new Object[] { "dalinar", "en_US" });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	@Test // DATAMONGO-1854
	public void shouldApplyCollationParameter() {

		Collation collation = Collation.of("en_US");
		createQueryForMethod("findWithCollationParameterByFirstName", String.class, Collation.class) //
				.execute(new Object[] { "dalinar", collation });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation()).contains(collation);
	}

	@Test // DATAMONGO-1854
	public void collationParameterShouldOverrideAnnotation() {

		Collation collation = Collation.of("de_AT");
		createQueryForMethod("findWithWithCollationParameterAndAnnotationByFirstName", String.class, Collation.class) //
				.execute(new Object[] { "dalinar", collation });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation()).contains(collation);
	}

	@Test // DATAMONGO-1854
	public void collationParameterShouldNotBeAppliedWhenNullOverrideAnnotation() {

		createQueryForMethod("findWithWithCollationParameterAndAnnotationByFirstName", String.class, Collation.class) //
				.execute(new Object[] { "dalinar", null });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(withQueryMock).matching(captor.capture());
		assertThat(captor.getValue().getCollation().map(Collation::toDocument))
				.contains(Collation.of("en_US").toDocument());
	}

	private MongoQueryFake createQueryForMethod(String methodName, Class<?>... paramTypes) {
		return createQueryForMethod(Repo.class, methodName, paramTypes);
	}

	private MongoQueryFake createQueryForMethod(Class<?> repository, String methodName, Class<?>... paramTypes) {

		try {

			Method method = repository.getMethod(methodName, paramTypes);
			ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
			MongoQueryMethod queryMethod = new MongoQueryMethod(method, new DefaultRepositoryMetadata(repository), factory,
					mappingContextMock);

			return new MongoQueryFake(queryMethod, mongoOperationsMock);
		} catch (Exception e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}

	private static class MongoQueryFake extends AbstractMongoQuery {

		private boolean isDeleteQuery;
		private boolean isLimitingQuery;

		public MongoQueryFake(MongoQueryMethod method, MongoOperations operations) {
			super(method, operations, new SpelExpressionParser(), QueryMethodEvaluationContextProvider.DEFAULT);
		}

		@Override
		protected Query createQuery(ConvertingParameterAccessor accessor) {
			return new BasicQuery("{'foo':'bar'}");
		}

		@Override
		protected boolean isCountQuery() {
			return false;
		}

		@Override
		protected boolean isExistsQuery() {
			return false;
		}

		@Override
		protected boolean isDeleteQuery() {
			return isDeleteQuery;
		}

		@Override
		protected boolean isLimiting() {
			return isLimitingQuery;
		}

		public MongoQueryFake setDeleteQuery(boolean isDeleteQuery) {
			this.isDeleteQuery = isDeleteQuery;
			return this;
		}

		public MongoQueryFake setLimitingQuery(boolean limitingQuery) {

			isLimitingQuery = limitingQuery;
			return this;
		}
	}

	private interface Repo extends MongoRepository<Person, Long> {

		List<Person> deleteByLastname(String lastname);

		Long deletePersonByLastname(String lastname);

		List<Person> findByFirstname(String firstname);

		@Meta(comment = "comment", flags = { org.springframework.data.mongodb.core.query.Meta.CursorOption.NO_TIMEOUT })
		Page<Person> findByFirstname(String firstnanme, Pageable pageable);

		@Meta(comment = "comment")
		@org.springframework.data.mongodb.repository.Query("{}")
		Page<Person> findByAnnotatedQuery(String firstnanme, Pageable pageable);

		// DATAMONGO-1057
		Slice<Person> findByLastname(String lastname, Pageable page);

		Optional<Person> findByLastname(String lastname);

		Person findFirstByLastname(String lastname);

		@org.springframework.data.mongodb.repository.Query(sort = "{ age : 1 }")
		List<Person> findByAge(Integer age);

		@org.springframework.data.mongodb.repository.Query(sort = "{ age : 1 }")
		List<Person> findByAge(Integer age, Sort page);

		@org.springframework.data.mongodb.repository.Query(collation = "en_US")
		List<Person> findWithCollationUsingSpimpleStringValueByFirstName(String firstname);

		@org.springframework.data.mongodb.repository.Query(collation = "{ 'locale' : 'en_US' }")
		List<Person> findWithCollationUsingDocumentByFirstName(String firstname);

		@org.springframework.data.mongodb.repository.Query(collation = "?1")
		List<Person> findWithCollationUsingPlaceholderByFirstName(String firstname, Object collation);

		@org.springframework.data.mongodb.repository.Query(collation = "{ 'locale' : '?1' }")
		List<Person> findWithCollationUsingPlaceholderInDocumentByFirstName(String firstname, String collation);

		List<Person> findWithCollationParameterByFirstName(String firstname, Collation collation);

		@org.springframework.data.mongodb.repository.Query(collation = "{ 'locale' : 'en_US' }")
		List<Person> findWithWithCollationParameterAndAnnotationByFirstName(String firstname, Collation collation);
	}

	// DATAMONGO-1872

	@org.springframework.data.mongodb.core.mapping.Document("#{T(java.lang.Math).random()}")
	static class DynamicallyMapped {}

	interface DynamicallyMappedRepository extends Repository<DynamicallyMapped, ObjectId> {
		DynamicallyMapped findBy();
	}
}
