package study.querydsl.repository;

import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.support.PageableExecutionUtils;
import study.querydsl.dto.MemberSearchCondition;
import study.querydsl.dto.MemberTeamDto;
import study.querydsl.dto.QMemberTeamDto;

import java.util.List;

import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.team;

@RequiredArgsConstructor
public class MemberRepositoryImpl implements MemberRepositoryCustom {

	private final JPAQueryFactory queryFactory;

	@Override
	public List<MemberTeamDto> search(MemberSearchCondition condition) {
		return queryFactory
				.select(new QMemberTeamDto(
						member.id.as("memberId"),
						member.username,
						member.age,
						team.id.as("teamId"),
						team.name.as("teamName")
				))
				.from(member)
				.leftJoin(member.team, team)
				.where(
						usernameEq(condition.getUsername()),
						teamNameEq(condition.getTeamName()),
						ageGoe(condition.getAgeGoe()),
						ageLoe(condition.getAgeLoe()))
				.fetch();
	}

	@Override
	public Page<MemberTeamDto> searchPageSimple(MemberSearchCondition condition, Pageable pageable) {
		List<MemberTeamDto> content = queryFactory
				.select(new QMemberTeamDto(
						member.id.as("memberId"),
						member.username,
						member.age,
						team.id.as("teamId"),
						team.name.as("teamName")
				))
				.from(member)
				.leftJoin(member.team, team)
				.where(
						usernameEq(condition.getUsername()),
						teamNameEq(condition.getTeamName()),
						ageGoe(condition.getAgeGoe()),
						ageLoe(condition.getAgeLoe()))
				.offset(pageable.getOffset())
				.limit(pageable.getPageSize())
				.fetch();

		JPAQuery<Long> countQuery = queryFactory
				.select(member.count())
				.from(member)
				.leftJoin(member.team, team)
				.where(
						usernameEq(condition.getUsername()),
						teamNameEq(condition.getTeamName()),
						ageGoe(condition.getAgeGoe()),
						ageLoe(condition.getAgeLoe()));

		return PageableExecutionUtils.getPage(content, pageable, countQuery::fetchOne);
	}

	@Override
	public Page<MemberTeamDto> searchPageComplex(MemberSearchCondition condition, Pageable pageable) {
		List<MemberTeamDto> content = queryFactory
				.select(new QMemberTeamDto(
						member.id.as("memberId"),
						member.username,
						member.age,
						team.id.as("teamId"),
						team.name.as("teamName")
				))
				.from(member)
				.leftJoin(member.team, team)
				.where(
						usernameEq(condition.getUsername()),
						teamNameEq(condition.getTeamName()),
						ageGoe(condition.getAgeGoe()),
						ageLoe(condition.getAgeLoe()))
				.offset(pageable.getOffset())
				.limit(pageable.getPageSize())
				.fetch();

		Long count = queryFactory
				.select(member.count())
				.from(member)
				.leftJoin(member.team, team)
				.where(
						usernameEq(condition.getUsername()),
						teamNameEq(condition.getTeamName()),
						ageGoe(condition.getAgeGoe()),
						ageLoe(condition.getAgeLoe())).fetchOne();

		return new PageImpl<>(content, pageable, count);
//		return PageableExecutionUtils.getPage(content, pageable, count::fetchOne);
	}

	private BooleanExpression usernameEq(String username) {
		if (username == null || username.isEmpty())
			return null;
		return member.username.eq(username);
	}

	private BooleanExpression teamNameEq(String teamName) {
		if (teamName == null || teamName.isEmpty())
			return null;
		return team.name.eq(teamName);
	}

	private BooleanExpression ageGoe(Integer ageGoe) {
		if (ageGoe == null)
			return null;
		return member.age.goe(ageGoe);
	}

	private BooleanExpression ageLoe(Integer ageLoe) {
		if (ageLoe == null)
			return null;
		return member.age.loe(ageLoe);
	}
}
