/**
 * Copyright (c) 2011 10gen Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "pch.h"
#include "db/pipeline/expression.h"

#include <cstdio>
#include <vector>

#include "mongo/db/jsobj.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/value.h"
#include "mongo/db/query/datetime/date_time_support.h"
#include "mongo/platform/bits.h"
#include "mongo/platform/decimal128.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/string_map.h"
#include "mongo/util/summation.h"

namespace mongo {
    using namespace mongoutils;

    /* --------------------------- Expression ------------------------------ */

    void Expression::toMatcherBson(BSONObjBuilder *pBuilder) const {
        verify(false && "Expression::toMatcherBson()");
    }

/// Helper function to easily wrap constants with $const.
static Value serializeConstant(Value val) {
    if (val.missing()) {
        return Value("$$REMOVE"_sd);
    }

    return Value(DOC("$const" << val));
}

        unsigned flag;
        static const unsigned FIXED_COUNT = 0x0001;
        static const unsigned OBJECT_ARG = 0x0002;

        unsigned argCount;
    };

    static int OpDescCmp(const void *pL, const void *pR) {
        return strcmp(((const OpDesc *)pL)->pName, ((const OpDesc *)pR)->pName);
    }

    /*
      Keep these sorted alphabetically so we can bsearch() them using
      OpDescCmp() above.
    */
    static const OpDesc OpTable[] = {
        {"$add", ExpressionAdd::create, 0},
        {"$and", ExpressionAnd::create, 0},
        {"$cmp", ExpressionCompare::createCmp, OpDesc::FIXED_COUNT, 2},
        {"$concat", ExpressionConcat::create, 0},
        {"$cond", ExpressionCond::create, OpDesc::FIXED_COUNT, 3},
        // $const handled specially in parseExpression
        {"$dayOfMonth", ExpressionDayOfMonth::create, OpDesc::FIXED_COUNT, 1},
        {"$dayOfWeek", ExpressionDayOfWeek::create, OpDesc::FIXED_COUNT, 1},
        {"$dayOfYear", ExpressionDayOfYear::create, OpDesc::FIXED_COUNT, 1},
        {"$divide", ExpressionDivide::create, OpDesc::FIXED_COUNT, 2},
        {"$eq", ExpressionCompare::createEq, OpDesc::FIXED_COUNT, 2},
        {"$gt", ExpressionCompare::createGt, OpDesc::FIXED_COUNT, 2},
        {"$gte", ExpressionCompare::createGte, OpDesc::FIXED_COUNT, 2},
        {"$hour", ExpressionHour::create, OpDesc::FIXED_COUNT, 1},
        {"$ifNull", ExpressionIfNull::create, OpDesc::FIXED_COUNT, 2},
        {"$lt", ExpressionCompare::createLt, OpDesc::FIXED_COUNT, 2},
        {"$lte", ExpressionCompare::createLte, OpDesc::FIXED_COUNT, 2},
        {"$millisecond", ExpressionMillisecond::create, OpDesc::FIXED_COUNT, 1},
        {"$minute", ExpressionMinute::create, OpDesc::FIXED_COUNT, 1},
        {"$mod", ExpressionMod::create, OpDesc::FIXED_COUNT, 2},
        {"$month", ExpressionMonth::create, OpDesc::FIXED_COUNT, 1},
        {"$multiply", ExpressionMultiply::create, 0},
        {"$ne", ExpressionCompare::createNe, OpDesc::FIXED_COUNT, 2},
        {"$not", ExpressionNot::create, OpDesc::FIXED_COUNT, 1},
        {"$or", ExpressionOr::create, 0},
        {"$second", ExpressionSecond::create, OpDesc::FIXED_COUNT, 1},
        {"$split", ExpressionSplit::create, OpDesc::FIXED_COUNT, 2},
        {"$strcasecmp", ExpressionStrcasecmp::create, OpDesc::FIXED_COUNT, 2},
        {"$substr", ExpressionSubstr::create, OpDesc::FIXED_COUNT, 3},
        {"$subtract", ExpressionSubtract::create, OpDesc::FIXED_COUNT, 2},
        {"$toLower", ExpressionToLower::create, OpDesc::FIXED_COUNT, 1},
        {"$toUpper", ExpressionToUpper::create, OpDesc::FIXED_COUNT, 1},
        {"$week", ExpressionWeek::create, OpDesc::FIXED_COUNT, 1},
        {"$year", ExpressionYear::create, OpDesc::FIXED_COUNT, 1},
    };

    static const size_t NOp = sizeof(OpTable)/sizeof(OpTable[0]);

    intrusive_ptr<Expression> Expression::parseExpression(
        const char *pOpName, BSONElement *pBsonElement) {
        /* look for the specified operator */

        if (str::equals(pOpName, "$const")) {
            return ExpressionConstant::createFromBsonElement(pBsonElement);
        }

        OpDesc key;
        key.pName = pOpName;
        const OpDesc *pOp = (const OpDesc *)bsearch(
                                &key, OpTable, NOp, sizeof(OpDesc), OpDescCmp);

        uassert(15999, str::stream() << "invalid operator '" <<
                pOpName << "'", pOp);

        /* make the expression node */
        intrusive_ptr<ExpressionNary> pExpression((*pOp->pFactory)());

        /* add the operands to the expression node */
        BSONType elementType = pBsonElement->type();

        if (pOp->flag & OpDesc::FIXED_COUNT) {
            if (pOp->argCount > 1)
                uassert(16019, str::stream() << "the " << pOp->pName <<
                        " operator requires an array of " << pOp->argCount <<
                        " operands", elementType == Array);
        }

        if (elementType == Object) {
            /* the operator must be unary and accept an object argument */
            uassert(16021, str::stream() << "the " << pOp->pName <<
                    " operator does not accept an object as an operand",
                    pOp->flag & OpDesc::OBJECT_ARG);

            BSONObj objOperand(pBsonElement->Obj());
            ObjectCtx oCtx(ObjectCtx::DOCUMENT_OK);
            intrusive_ptr<Expression> pOperand(
                Expression::parseObject(pBsonElement, &oCtx));
            pExpression->addOperand(pOperand);
        }
        else if (elementType == Array) {
            /* multiple operands - an n-ary operator */
            vector<BSONElement> bsonArray(pBsonElement->Array());
            const size_t n = bsonArray.size();

            if (pOp->flag & OpDesc::FIXED_COUNT)
                uassert(16020, str::stream() << "the " << pOp->pName <<
                        " operator requires " << pOp->argCount <<
                        " operand(s)", pOp->argCount == n);

            for(size_t i = 0; i < n; ++i) {
                BSONElement *pBsonOperand = &bsonArray[i];
                intrusive_ptr<Expression> pOperand(
                    Expression::parseOperand(pBsonOperand));
                pExpression->addOperand(pOperand);
            }
        }
        else {
            /* assume it's an atomic operand */
            if (pOp->flag & OpDesc::FIXED_COUNT)
                uassert(16022, str::stream() << "the " << pOp->pName <<
                        " operator requires an array of " << pOp->argCount <<
                        " operands", pOp->argCount == 1);

            intrusive_ptr<Expression> pOperand(
                Expression::parseOperand(pBsonElement));
            pExpression->addOperand(pOperand);
        }

        return pExpression;
    }

    intrusive_ptr<Expression> Expression::parseOperand(BSONElement *pBsonElement) {
        BSONType type = pBsonElement->type();

        if (type == String && pBsonElement->valuestr()[0] == '$') {
            /* if we got here, this is a field path expression */
            string fieldPath = removeFieldPrefix(pBsonElement->str());
            return ExpressionFieldPath::create(fieldPath);
        }
        else if (type == Object) {
            ObjectCtx oCtx(ObjectCtx::DOCUMENT_OK);
            return Expression::parseObject(pBsonElement, &oCtx);
        }
        else {
            return ExpressionConstant::createFromBsonElement(pBsonElement);
        }
    }

    /* ------------------------- ExpressionAdd ----------------------------- */

    ExpressionAdd::~ExpressionAdd() {
    }

    invariant(isLeadingByte(charByte));

    // In UTF-8, the number of leading ones is the number of bytes the code point takes up.
    return countLeadingZeros64(~(uint64_t(charByte) << (64 - 8)));
}
}  // namespace

/* ------------------------- Register Date Expressions ----------------------------- */

REGISTER_EXPRESSION(dayOfMonth, ExpressionDayOfMonth::parse);
REGISTER_EXPRESSION(dayOfWeek, ExpressionDayOfWeek::parse);
REGISTER_EXPRESSION(dayOfYear, ExpressionDayOfYear::parse);
REGISTER_EXPRESSION(hour, ExpressionHour::parse);
REGISTER_EXPRESSION(isoDayOfWeek, ExpressionIsoDayOfWeek::parse);
REGISTER_EXPRESSION(isoWeek, ExpressionIsoWeek::parse);
REGISTER_EXPRESSION(isoWeekYear, ExpressionIsoWeekYear::parse);
REGISTER_EXPRESSION(millisecond, ExpressionMillisecond::parse);
REGISTER_EXPRESSION(minute, ExpressionMinute::parse);
REGISTER_EXPRESSION(month, ExpressionMonth::parse);
REGISTER_EXPRESSION(second, ExpressionSecond::parse);
REGISTER_EXPRESSION(week, ExpressionWeek::parse);
REGISTER_EXPRESSION(year, ExpressionYear::parse);

/* ----------------------- ExpressionAbs ---------------------------- */

Value ExpressionAbs::evaluateNumericArg(const Value& numericArg) const {
    BSONType type = numericArg.getType();
    if (type == NumberDouble) {
        return Value(std::abs(numericArg.getDouble()));
    } else if (type == NumberDecimal) {
        return Value(numericArg.getDecimal().toAbs());
    } else {
        long long num = numericArg.getLong();
        uassert(28680,
                "can't take $abs of long long min",
                num != std::numeric_limits<long long>::min());
        long long absVal = std::abs(num);
        return type == NumberLong ? Value(absVal) : Value::createIntOrLong(absVal);
    }

    Value ExpressionAdd::evaluate(const Document& pDocument) const {

        /*
          We'll try to return the narrowest possible result value.  To do that
          without creating intermediate Values, do the arithmetic for double
          and integral types in parallel, tracking the current narrowest
          type.
         */
        double doubleTotal = 0;
        long long longTotal = 0;
        BSONType totalType = NumberInt;
        bool haveDate = false;

Value ExpressionAdd::evaluate(const Document& root) const {
    // We'll try to return the narrowest possible result value while avoiding overflow, loss
    // of precision due to intermediate rounding or implicit use of decimal types. To do that,
    // compute a compensated sum for non-decimal values and a separate decimal sum for decimal
    // values, and track the current narrowest type.
    DoubleDoubleSummation nonDecimalTotal;
    Decimal128 decimalTotal;
    BSONType totalType = NumberInt;
    bool haveDate = false;

    const size_t n = vpOperand.size();
    for (size_t i = 0; i < n; ++i) {
        Value val = vpOperand[i]->evaluate(root);

                doubleTotal += val.coerceToDouble();
                longTotal += val.coerceToLong();
            }
            else if (val.getType() == Date) {
                uassert(16612, "only one Date allowed in an $add expression",
                        !haveDate);
                haveDate = true;
                nonDecimalTotal.addLong(val.getDate().toMillisSinceEpoch());
                break;
            default:
                uassert(16554,
                        str::stream() << "$add only supports numeric or date types, not "
                                      << typeName(val.getType()),
                        val.nullish());
                return Value(BSONNULL);
            }
            else {
                uasserted(16554, str::stream() << "$add only supports numeric or date types, not "
                                               << typeName(val.getType()));
            }
        }

        if (haveDate) {
            if (totalType == NumberDouble)
                longTotal = static_cast<long long>(doubleTotal);
            return Value::createDate(longTotal);
        }
        else if (totalType == NumberLong) {
            return Value::createLong(longTotal);
        }
        else if (totalType == NumberDouble) {
            return Value::createDouble(doubleTotal);
        }
        else if (totalType == NumberInt) {
            return Value::createIntOrLong(longTotal);
        }
        else {
            massert(16417, "$add resulted in a non-numeric type", false);
        }
    }

    const char *ExpressionAdd::getOpName() const {
        return "$add";
    }

/* ------------------------- ExpressionAllElementsTrue -------------------------- */

Value ExpressionAllElementsTrue::evaluate(const Document& root) const {
    const Value arr = vpOperand[0]->evaluate(root);
    uassert(17040,
            str::stream() << getOpName() << "'s argument must be an array, but is "
                          << typeName(arr.getType()),
            arr.isArray());
    const vector<Value>& array = arr.getArray();
    for (vector<Value>::const_iterator it = array.begin(); it != array.end(); ++it) {
        if (!it->coerceToBool()) {
            return Value(false);
        }
    }

    /* ------------------------- ExpressionAnd ----------------------------- */

    ExpressionAnd::~ExpressionAnd() {
    }

    intrusive_ptr<ExpressionNary> ExpressionAnd::create() {
        intrusive_ptr<ExpressionNary> pExpression(new ExpressionAnd());
        return pExpression;
    }

    ExpressionAnd::ExpressionAnd():
        ExpressionNary() {
    }

    intrusive_ptr<Expression> ExpressionAnd::optimize() {
        /* optimize the conjunction as much as possible */
        intrusive_ptr<Expression> pE(ExpressionNary::optimize());

        /* if the result isn't a conjunction, we can't do anything */
        ExpressionAnd *pAnd = dynamic_cast<ExpressionAnd *>(pE.get());
        if (!pAnd)
            return pE;

        /*
          Check the last argument on the result; if it's not constant (as
          promised by ExpressionNary::optimize(),) then there's nothing
          we can do.
        */
        const size_t n = pAnd->vpOperand.size();
        // ExpressionNary::optimize() generates an ExpressionConstant for {$and:[]}.
        verify(n > 0);
        intrusive_ptr<Expression> pLast(pAnd->vpOperand[n - 1]);
        const ExpressionConstant *pConst =
            dynamic_cast<ExpressionConstant *>(pLast.get());
        if (!pConst)
            return pE;

        /*
          Evaluate and coerce the last argument to a boolean.  If it's false,
          then we can replace this entire expression.
         */
        bool last = pLast->evaluate(Document()).coerceToBool();
        if (!last) {
            intrusive_ptr<ExpressionConstant> pFinal(
                ExpressionConstant::create(Value(false)));
            return pFinal;
        }

        /*
          If we got here, the final operand was true, so we don't need it
          anymore.  If there was only one other operand, we don't need the
          conjunction either.  Note we still need to keep the promise that
          the result will be a boolean.
         */
        if (n == 2) {
            intrusive_ptr<Expression> pFinal(
                ExpressionCoerceToBool::create(pAnd->vpOperand[0]));
            return pFinal;
        }

        /*
          Remove the final "true" value, and return the new expression.

          CW TODO:
          Note that because of any implicit conversions, we may need to
          apply an implicit boolean conversion.
        */
        pAnd->vpOperand.resize(n - 1);
        return pE;
    }

    Value ExpressionAnd::evaluate(const Document& pDocument) const {
        const size_t n = vpOperand.size();
        for(size_t i = 0; i < n; ++i) {
            Value pValue(vpOperand[i]->evaluate(pDocument));
            if (!pValue.coerceToBool())
                return Value(false);
        }

Value ExpressionAnd::evaluate(const Document& root) const {
    const size_t n = vpOperand.size();
    for (size_t i = 0; i < n; ++i) {
        Value pValue(vpOperand[i]->evaluate(root));
        if (!pValue.coerceToBool())
            return Value(false);
    }

    const char *ExpressionAnd::getOpName() const {
        return "$and";
    }

    void ExpressionAnd::toMatcherBson(BSONObjBuilder *pBuilder) const {
        /*
          There are two patterns we can handle:
          (1) one or two comparisons on the same field: { a:{$gte:3, $lt:7} }
          (2) multiple field comparisons: {a:7, b:{$lte:6}, c:2}
            This can be recognized as a conjunction of a set of  range
            expressions.  Direct equality is a degenerate range expression;
            range expressions can be open-ended.
        */
        verify(false && "unimplemented");
    }

/* ------------------------- ExpressionAnyElementTrue -------------------------- */

Value ExpressionAnyElementTrue::evaluate(const Document& root) const {
    const Value arr = vpOperand[0]->evaluate(root);
    uassert(17041,
            str::stream() << getOpName() << "'s argument must be an array, but is "
                          << typeName(arr.getType()),
            arr.isArray());
    const vector<Value>& array = arr.getArray();
    for (vector<Value>::const_iterator it = array.begin(); it != array.end(); ++it) {
        if (it->coerceToBool()) {
            return Value(true);
        }
    }

    /* -------------------- ExpressionCoerceToBool ------------------------- */

    ExpressionCoerceToBool::~ExpressionCoerceToBool() {
    }

Value ExpressionArray::evaluate(const Document& root) const {
    vector<Value> values;
    values.reserve(vpOperand.size());
    for (auto&& expr : vpOperand) {
        Value elemVal = expr->evaluate(root);
        values.push_back(elemVal.missing() ? Value(BSONNULL) : std::move(elemVal));
    }

    ExpressionCoerceToBool::ExpressionCoerceToBool(
        const intrusive_ptr<Expression> &pTheExpression):
        Expression(),
        pExpression(pTheExpression) {
    }

    intrusive_ptr<Expression> ExpressionCoerceToBool::optimize() {
        /* optimize the operand */
        pExpression = pExpression->optimize();

/* ------------------------- ExpressionArrayElemAt -------------------------- */

Value ExpressionArrayElemAt::evaluate(const Document& root) const {
    const Value array = vpOperand[0]->evaluate(root);
    const Value indexArg = vpOperand[1]->evaluate(root);

    if (array.nullish() || indexArg.nullish()) {
        return Value(BSONNULL);
    }

    uassert(28689,
            str::stream() << getOpName() << "'s first argument must be an array, but is "
                          << typeName(array.getType()),
            array.isArray());
    uassert(28690,
            str::stream() << getOpName() << "'s second argument must be a numeric value,"
                          << " but is "
                          << typeName(indexArg.getType()),
            indexArg.numeric());
    uassert(28691,
            str::stream() << getOpName() << "'s second argument must be representable as"
                          << " a 32-bit integer: "
                          << indexArg.coerceToDouble(),
            indexArg.integral());

    long long i = indexArg.coerceToLong();
    if (i < 0 && static_cast<size_t>(std::abs(i)) > array.getArrayLength()) {
        // Positive indices that are too large are handled automatically by Value.
        return Value();
    } else if (i < 0) {
        // Index from the back of the array.
        i = array.getArrayLength() + i;
    }
    const size_t index = static_cast<size_t>(i);
    return array[index];
}

        return intrusive_ptr<Expression>(this);
    }

/* ------------------------- ExpressionObjectToArray -------------------------- */

Value ExpressionObjectToArray::evaluate(const Document& root) const {
    const Value targetVal = vpOperand[0]->evaluate(root);

    if (targetVal.nullish()) {
        return Value(BSONNULL);
    }

    uassert(40390,
            str::stream() << "$objectToArray requires a document input, found: "
                          << typeName(targetVal.getType()),
            (targetVal.getType() == BSONType::Object));

    vector<Value> output;

    FieldIterator iter = targetVal.getDocument().fieldIterator();
    while (iter.more()) {
        Document::FieldPair pair = iter.next();
        MutableDocument keyvalue;
        keyvalue.addField("k", Value(pair.first));
        keyvalue.addField("v", pair.second);
        output.push_back(keyvalue.freezeToValue());
    }

    return Value(output);
}

REGISTER_EXPRESSION(objectToArray, ExpressionObjectToArray::parse);
const char* ExpressionObjectToArray::getOpName() const {
    return "$objectToArray";
}

/* ------------------------- ExpressionArrayToObject -------------------------- */
Value ExpressionArrayToObject::evaluate(const Document& root) const {
    const Value input = vpOperand[0]->evaluate(root);
    if (input.nullish()) {
        return Value(BSONNULL);
    }

    uassert(40386,
            str::stream() << "$arrayToObject requires an array input, found: "
                          << typeName(input.getType()),
            input.isArray());

    MutableDocument output;
    const vector<Value>& array = input.getArray();
    if (array.empty()) {
        return output.freezeToValue();
    }

    // There are two accepted input formats in an array: [ [key, val] ] or [ {k:key, v:val} ]. The
    // first array element determines the format for the rest of the array. Mixing input formats is
    // not allowed.
    bool inputArrayFormat;
    if (array[0].isArray()) {
        inputArrayFormat = true;
    } else if (array[0].getType() == BSONType::Object) {
        inputArrayFormat = false;
    } else {
        uasserted(40398,
                  str::stream() << "Unrecognised input type format for $arrayToObject: "
                                << typeName(array[0].getType()));
    }

    for (auto&& elem : array) {
        if (inputArrayFormat == true) {
            uassert(
                40396,
                str::stream() << "$arrayToObject requires a consistent input format. Elements must"
                                 "all be arrays or all be objects. Array was detected, now found: "
                              << typeName(elem.getType()),
                elem.isArray());

            const vector<Value>& valArray = elem.getArray();

            uassert(40397,
                    str::stream() << "$arrayToObject requires an array of size 2 arrays,"
                                     "found array of size: "
                                  << valArray.size(),
                    (valArray.size() == 2));

            uassert(40395,
                    str::stream() << "$arrayToObject requires an array of key-value pairs, where "
                                     "the key must be of type string. Found key type: "
                                  << typeName(valArray[0].getType()),
                    (valArray[0].getType() == BSONType::String));

            output.addField(valArray[0].getString(), valArray[1]);

        } else {
            uassert(
                40391,
                str::stream() << "$arrayToObject requires a consistent input format. Elements must"
                                 "all be arrays or all be objects. Object was detected, now found: "
                              << typeName(elem.getType()),
                (elem.getType() == BSONType::Object));

            uassert(40392,
                    str::stream() << "$arrayToObject requires an object keys of 'k' and 'v'. "
                                     "Found incorrect number of keys:"
                                  << elem.getDocument().size(),
                    (elem.getDocument().size() == 2));

            Value key = elem.getDocument().getField("k");
            Value value = elem.getDocument().getField("v");

            uassert(40393,
                    str::stream() << "$arrayToObject requires an object with keys 'k' and 'v'. "
                                     "Missing either or both keys from: "
                                  << elem.toString(),
                    (!key.missing() && !value.missing()));

            uassert(
                40394,
                str::stream() << "$arrayToObject requires an object with keys 'k' and 'v', where "
                                 "the value of 'k' must be of type string. Found type: "
                              << typeName(key.getType()),
                (key.getType() == BSONType::String));

            output.addField(key.getString(), value);
        }
    }

    return output.freezeToValue();
}

REGISTER_EXPRESSION(arrayToObject, ExpressionArrayToObject::parse);
const char* ExpressionArrayToObject::getOpName() const {
    return "$arrayToObject";
}

/* ------------------------- ExpressionCeil -------------------------- */

Value ExpressionCeil::evaluateNumericArg(const Value& numericArg) const {
    // There's no point in taking the ceiling of integers or longs, it will have no effect.
    switch (numericArg.getType()) {
        case NumberDouble:
            return Value(std::ceil(numericArg.getDouble()));
        case NumberDecimal:
            // Round toward the nearest decimal with a zero exponent in the positive direction.
            return Value(numericArg.getDecimal().quantize(Decimal128::kNormalizedZero,
                                                          Decimal128::kRoundTowardPositive));
        default:
            return numericArg;
    }

    Value ExpressionCoerceToBool::evaluate(const Document& pDocument) const {
        Value pResult(pExpression->evaluate(pDocument));
        bool b = pResult.coerceToBool();
        if (b)
            return Value(true);
        return Value(false);
    }

    void ExpressionCoerceToBool::addToBsonObj(BSONObjBuilder *pBuilder,
                                              StringData fieldName,
                                              bool requireExpression) const {
        // Serializing as an $and expression which will become a CoerceToBool
        BSONObjBuilder sub (pBuilder->subobjStart(fieldName));
        BSONArrayBuilder arr (sub.subarrayStart("$and"));
        pExpression->addToBsonArray(&arr);
        arr.doneFast();
        sub.doneFast();
    }

    void ExpressionCoerceToBool::addToBsonArray(
            BSONArrayBuilder *pBuilder) const {
        // Serializing as an $and expression which will become a CoerceToBool
        BSONObjBuilder sub (pBuilder->subobjStart());
        BSONArrayBuilder arr (sub.subarrayStart("$and"));
        pExpression->addToBsonArray(&arr);
        arr.doneFast();
        sub.doneFast();
    }

    /* ----------------------- ExpressionCompare --------------------------- */

    ExpressionCompare::~ExpressionCompare() {
    }

    intrusive_ptr<ExpressionNary> ExpressionCompare::createEq() {
        intrusive_ptr<ExpressionCompare> pExpression(
            new ExpressionCompare(EQ));
        return pExpression;
    }

    intrusive_ptr<ExpressionNary> ExpressionCompare::createNe() {
        intrusive_ptr<ExpressionCompare> pExpression(
            new ExpressionCompare(NE));
        return pExpression;
    }

    intrusive_ptr<ExpressionNary> ExpressionCompare::createGt() {
        intrusive_ptr<ExpressionCompare> pExpression(
            new ExpressionCompare(GT));
        return pExpression;
    }

    intrusive_ptr<ExpressionNary> ExpressionCompare::createGte() {
        intrusive_ptr<ExpressionCompare> pExpression(
            new ExpressionCompare(GTE));
        return pExpression;
    }

Value ExpressionCoerceToBool::evaluate(const Document& root) const {
    Value pResult(pExpression->evaluate(root));
    bool b = pResult.coerceToBool();
    if (b)
        return Value(true);
    return Value(false);
}

    intrusive_ptr<ExpressionNary> ExpressionCompare::createLte() {
        intrusive_ptr<ExpressionCompare> pExpression(
            new ExpressionCompare(LTE));
        return pExpression;
    }

    intrusive_ptr<ExpressionNary> ExpressionCompare::createCmp() {
        intrusive_ptr<ExpressionCompare> pExpression(
            new ExpressionCompare(CMP));
        return pExpression;
    }

    ExpressionCompare::ExpressionCompare(CmpOp theCmpOp):
        ExpressionNary(),
        cmpOp(theCmpOp) {
    }

    void ExpressionCompare::addOperand(
        const intrusive_ptr<Expression> &pExpression) {
        checkArgLimit(2);
        ExpressionNary::addOperand(pExpression);
    }

Value ExpressionCompare::evaluate(const Document& root) const {
    Value pLeft(vpOperand[0]->evaluate(root));
    Value pRight(vpOperand[1]->evaluate(root));

    intrusive_ptr<Expression> ExpressionCompare::optimize() {
        /* first optimize the comparison operands */
        intrusive_ptr<Expression> pE(ExpressionNary::optimize());

        /*
          If the result of optimization is no longer a comparison, there's
          nothing more we can do.
        */
        ExpressionCompare *pCmp = dynamic_cast<ExpressionCompare *>(pE.get());
        if (!pCmp)
            return pE;

        /* check to see if optimizing comparison operator is supported */
        CmpOp newOp = pCmp->cmpOp;
        // CMP and NE cannot use ExpressionFieldRange which is what this optimization uses
        if (newOp == CMP || newOp == NE)
            return pE;

        /*
          There's one localized optimization we recognize:  a comparison
          between a field and a constant.  If we recognize that pattern,
          replace it with an ExpressionFieldRange.

          When looking for this pattern, note that the operands could appear
          in any order.  If we need to reverse the sense of the comparison to
          put it into the required canonical form, do so.
         */
        intrusive_ptr<Expression> pLeft(pCmp->vpOperand[0]);
        intrusive_ptr<Expression> pRight(pCmp->vpOperand[1]);
        intrusive_ptr<ExpressionFieldPath> pFieldPath(
            dynamic_pointer_cast<ExpressionFieldPath>(pLeft));
        intrusive_ptr<ExpressionConstant> pConstant;
        if (pFieldPath.get()) {
            pConstant = dynamic_pointer_cast<ExpressionConstant>(pRight);
            if (!pConstant.get())
                return pE; // there's nothing more we can do
        }
        else {
            /* if the first operand wasn't a path, see if it's a constant */
            pConstant = dynamic_pointer_cast<ExpressionConstant>(pLeft);
            if (!pConstant.get())
                return pE; // there's nothing more we can do

            /* the left operand was a constant; see if the right is a path */
            pFieldPath = dynamic_pointer_cast<ExpressionFieldPath>(pRight);
            if (!pFieldPath.get())
                return pE; // there's nothing more we can do

            /* these were not in canonical order, so reverse the sense */
            newOp = cmpLookup[newOp].reverse;
        }

        return ExpressionFieldRange::create(
            pFieldPath, newOp, pConstant->getValue());
    }

    Value ExpressionCompare::evaluate(const Document& pDocument) const {
        checkArgCount(2);
        Value pLeft(vpOperand[0]->evaluate(pDocument));
        Value pRight(vpOperand[1]->evaluate(pDocument));

        int cmp = signum(Value::compare(pLeft, pRight));

        if (cmpOp == CMP) {
            switch(cmp) {
            case -1:
            case 0:
            case 1:
                return Value(cmp);

            default:
                verify(false); // CW TODO internal error
            }
        }

Value ExpressionConcat::evaluate(const Document& root) const {
    const size_t n = vpOperand.size();

    StringBuilder result;
    for (size_t i = 0; i < n; ++i) {
        Value val = vpOperand[i]->evaluate(root);
        if (val.nullish())
            return Value(BSONNULL);

    /* ------------------------- ExpressionConcat ----------------------------- */

    ExpressionConcat::~ExpressionConcat() {
    }

    intrusive_ptr<ExpressionNary> ExpressionConcat::create() {
        return new ExpressionConcat();
    }

    Value ExpressionConcat::evaluate(const Document& input) const {
        const size_t n = vpOperand.size();

        StringBuilder result;
        for (size_t i = 0; i < n; ++i) {
            Value val = vpOperand[i]->evaluate(input);
            if (val.nullish())
                return Value(BSONNULL);

Value ExpressionConcatArrays::evaluate(const Document& root) const {
    const size_t n = vpOperand.size();
    vector<Value> values;

    for (size_t i = 0; i < n; ++i) {
        Value val = vpOperand[i]->evaluate(root);
        if (val.nullish()) {
            return Value(BSONNULL);
        }

        return Value::createString(result.str());
    }

    const char *ExpressionConcat::getOpName() const {
        return "$concat";
    }

    /* ----------------------- ExpressionCond ------------------------------ */

    ExpressionCond::~ExpressionCond() {
    }

Value ExpressionCond::evaluate(const Document& root) const {
    Value pCond(vpOperand[0]->evaluate(root));
    int idx = pCond.coerceToBool() ? 1 : 2;
    return vpOperand[idx]->evaluate(root);
}

    ExpressionCond::ExpressionCond():
        ExpressionNary() {
    }

    void ExpressionCond::addOperand(
        const intrusive_ptr<Expression> &pExpression) {
        checkArgLimit(3);
        ExpressionNary::addOperand(pExpression);
    }

    Value ExpressionCond::evaluate(const Document& pDocument) const {
        checkArgCount(3);
        Value pCond(vpOperand[0]->evaluate(pDocument));
        int idx = pCond.coerceToBool() ? 1 : 2;
        return vpOperand[idx]->evaluate(pDocument);
    }

    const char *ExpressionCond::getOpName() const {
        return "$cond";
    }

    /* ---------------------- ExpressionConstant --------------------------- */

    ExpressionConstant::~ExpressionConstant() {
    }

    intrusive_ptr<ExpressionConstant> ExpressionConstant::createFromBsonElement(
        BSONElement *pBsonElement) {
        intrusive_ptr<ExpressionConstant> pEC(
            new ExpressionConstant(pBsonElement));
        return pEC;
    }

intrusive_ptr<ExpressionConstant> ExpressionConstant::create(
    const intrusive_ptr<ExpressionContext>& expCtx, const Value& value) {
    intrusive_ptr<ExpressionConstant> pEC(new ExpressionConstant(expCtx, value));
    return pEC;
}

ExpressionConstant::ExpressionConstant(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                       const Value& value)
    : Expression(expCtx), _value(value) {}

    ExpressionConstant::ExpressionConstant(const Value& pTheValue): pValue(pTheValue) {}


    intrusive_ptr<Expression> ExpressionConstant::optimize() {
        /* nothing to do */
        return intrusive_ptr<Expression>(this);
    }

Value ExpressionConstant::evaluate(const Document& root) const {
    return _value;
}

Value ExpressionConstant::serialize(bool explain) const {
    return serializeConstant(_value);
}

    void ExpressionConstant::addToBsonObj(BSONObjBuilder *pBuilder,
                                          StringData fieldName,
                                          bool requireExpression) const {
        /*
          If we don't need an expression, but can use a naked scalar,
          do the regular thing.

          This is geared to handle $project, which uses expressions as a cue
          that the field is a new virtual field rather than just an
          inclusion (or exclusion):
          { $project : {
              x : true, // include
              y : { $const: true }
          }}

/* ---------------------- ExpressionDateFromParts ----------------------- */

/* Helper functions also shared with ExpressionDateToParts */

namespace {

boost::optional<TimeZone> makeTimeZone(const TimeZoneDatabase* tzdb,
                                       const Document& root,
                                       intrusive_ptr<Expression> _timeZone) {
    if (!_timeZone) {
        return mongo::TimeZoneDatabase::utcZone();
    }

    auto timeZoneId = _timeZone->evaluate(root);

    if (timeZoneId.nullish()) {
        return boost::none;
    }

    uassert(40517,
            str::stream() << "timezone must evaluate to a string, found "
                          << typeName(timeZoneId.getType()),
            timeZoneId.getType() == BSONType::String);

    return tzdb->getTimeZone(timeZoneId.getString());
}

}  // namespace


REGISTER_EXPRESSION(dateFromParts, ExpressionDateFromParts::parse);
intrusive_ptr<Expression> ExpressionDateFromParts::parse(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    BSONElement expr,
    const VariablesParseState& vps) {

    uassert(40519,
            "$dateFromParts only supports an object as its argument",
            expr.type() == BSONType::Object);

    BSONElement yearElem;
    BSONElement monthElem;
    BSONElement dayElem;
    BSONElement hourElem;
    BSONElement minuteElem;
    BSONElement secondElem;
    BSONElement millisecondsElem;
    BSONElement isoYearElem;
    BSONElement isoWeekYearElem;
    BSONElement isoDayOfWeekElem;
    BSONElement timeZoneElem;

    const BSONObj args = expr.embeddedObject();
    for (auto&& arg : args) {
        auto field = arg.fieldNameStringData();

        if (field == "year"_sd) {
            yearElem = arg;
        } else if (field == "month"_sd) {
            monthElem = arg;
        } else if (field == "day"_sd) {
            dayElem = arg;
        } else if (field == "hour"_sd) {
            hourElem = arg;
        } else if (field == "minute"_sd) {
            minuteElem = arg;
        } else if (field == "second"_sd) {
            secondElem = arg;
        } else if (field == "milliseconds"_sd) {
            millisecondsElem = arg;
        } else if (field == "isoYear"_sd) {
            isoYearElem = arg;
        } else if (field == "isoWeekYear"_sd) {
            isoWeekYearElem = arg;
        } else if (field == "isoDayOfWeek"_sd) {
            isoDayOfWeekElem = arg;
        } else if (field == "timezone"_sd) {
            timeZoneElem = arg;
        } else {
            uasserted(40518,
                      str::stream() << "Unrecognized argument to $dateFromParts: "
                                    << arg.fieldName());
        }

    if (!yearElem && !isoYearElem) {
        uasserted(40516, "$dateFromParts requires either 'year' or 'isoYear' to be present");
    }

    if (yearElem && (isoYearElem || isoWeekYearElem || isoDayOfWeekElem)) {
        uasserted(40489, "$dateFromParts does not allow mixing natural dates with ISO dates");
    }

    if (isoYearElem && (yearElem || monthElem || dayElem)) {
        uasserted(40525, "$dateFromParts does not allow mixing ISO dates with natural dates");
    }

    return new ExpressionDateFromParts(
        expCtx,
        yearElem ? parseOperand(expCtx, yearElem, vps) : nullptr,
        monthElem ? parseOperand(expCtx, monthElem, vps) : nullptr,
        dayElem ? parseOperand(expCtx, dayElem, vps) : nullptr,
        hourElem ? parseOperand(expCtx, hourElem, vps) : nullptr,
        minuteElem ? parseOperand(expCtx, minuteElem, vps) : nullptr,
        secondElem ? parseOperand(expCtx, secondElem, vps) : nullptr,
        millisecondsElem ? parseOperand(expCtx, millisecondsElem, vps) : nullptr,
        isoYearElem ? parseOperand(expCtx, isoYearElem, vps) : nullptr,
        isoWeekYearElem ? parseOperand(expCtx, isoWeekYearElem, vps) : nullptr,
        isoDayOfWeekElem ? parseOperand(expCtx, isoDayOfWeekElem, vps) : nullptr,
        timeZoneElem ? parseOperand(expCtx, timeZoneElem, vps) : nullptr);
}

ExpressionDateFromParts::ExpressionDateFromParts(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    intrusive_ptr<Expression> year,
    intrusive_ptr<Expression> month,
    intrusive_ptr<Expression> day,
    intrusive_ptr<Expression> hour,
    intrusive_ptr<Expression> minute,
    intrusive_ptr<Expression> second,
    intrusive_ptr<Expression> milliseconds,
    intrusive_ptr<Expression> isoYear,
    intrusive_ptr<Expression> isoWeekYear,
    intrusive_ptr<Expression> isoDayOfWeek,
    intrusive_ptr<Expression> timeZone)
    : Expression(expCtx),
      _year(year),
      _month(month),
      _day(day),
      _hour(hour),
      _minute(minute),
      _second(second),
      _milliseconds(milliseconds),
      _isoYear(isoYear),
      _isoWeekYear(isoWeekYear),
      _isoDayOfWeek(isoDayOfWeek),
      _timeZone(timeZone) {}

intrusive_ptr<Expression> ExpressionDateFromParts::optimize() {
    if (_year) {
        _year = _year->optimize();
    }
    if (_month) {
        _month = _month->optimize();
    }
    if (_day) {
        _day = _day->optimize();
    }
    if (_hour) {
        _hour = _hour->optimize();
    }
    if (_minute) {
        _minute = _minute->optimize();
    }
    if (_second) {
        _second = _second->optimize();
    }
    if (_milliseconds) {
        _milliseconds = _milliseconds->optimize();
    }
    if (_isoYear) {
        _isoYear = _isoYear->optimize();
    }
    if (_isoWeekYear) {
        _isoWeekYear = _isoWeekYear->optimize();
    }
    if (_isoDayOfWeek) {
        _isoDayOfWeek = _isoDayOfWeek->optimize();
    }
    if (_timeZone) {
        _timeZone = _timeZone->optimize();
    }

    if (ExpressionConstant::allNullOrConstant({_year,
                                               _month,
                                               _day,
                                               _hour,
                                               _minute,
                                               _second,
                                               _milliseconds,
                                               _isoYear,
                                               _isoWeekYear,
                                               _isoDayOfWeek,
                                               _timeZone})) {
        // Everything is a constant, so we can turn into a constant.
        return ExpressionConstant::create(getExpressionContext(), evaluate(Document{}));
    }

    return this;
}

Value ExpressionDateFromParts::serialize(bool explain) const {
    return Value(Document{
        {"$dateFromParts",
         Document{{"year", _year ? _year->serialize(explain) : Value()},
                  {"month", _month ? _month->serialize(explain) : Value()},
                  {"day", _day ? _day->serialize(explain) : Value()},
                  {"hour", _hour ? _hour->serialize(explain) : Value()},
                  {"minute", _minute ? _minute->serialize(explain) : Value()},
                  {"second", _second ? _second->serialize(explain) : Value()},
                  {"milliseconds", _milliseconds ? _milliseconds->serialize(explain) : Value()},
                  {"isoYear", _isoYear ? _isoYear->serialize(explain) : Value()},
                  {"isoWeekYear", _isoWeekYear ? _isoWeekYear->serialize(explain) : Value()},
                  {"isoDayOfWeek", _isoDayOfWeek ? _isoDayOfWeek->serialize(explain) : Value()},
                  {"timezone", _timeZone ? _timeZone->serialize(explain) : Value()}}}});
}

/**
 * This function checks whether a field is a number, and fits in the given range.
 *
 * If the field does not exist, the default value is returned trough the returnValue out parameter
 * and the function returns true.
 *
 * If the field exists:
 * - if the value is "nullish", the function returns false, so that the calling function can return
 *   a BSONNULL value.
 * - if the value can not be coerced to an integral value, an exception is returned.
 * - if the value is out of the range [minValue..maxValue], an exception is returned.
 * - otherwise, the coerced integral value is returned through the returnValue
 *   out parameter, and the function returns true.
 */
bool ExpressionDateFromParts::evaluateNumberWithinRange(const Document& root,
                                                        intrusive_ptr<Expression> field,
                                                        StringData fieldName,
                                                        int defaultValue,
                                                        int minValue,
                                                        int maxValue,
                                                        int* returnValue) const {
    if (!field) {
        *returnValue = defaultValue;
        return true;
    }

    auto fieldValue = field->evaluate(root);

    if (fieldValue.nullish()) {
        return false;
    }

    uassert(40515,
            str::stream() << "'" << fieldName << "' must evaluate to an integer, found "
                          << typeName(fieldValue.getType())
                          << " with value "
                          << fieldValue.toString(),
            fieldValue.integral());

    *returnValue = fieldValue.coerceToInt();

    uassert(40523,
            str::stream() << "'" << fieldName << "' must evaluate to an integer in the range "
                          << minValue
                          << " to "
                          << maxValue
                          << ", found "
                          << *returnValue,
            *returnValue >= minValue && *returnValue <= maxValue);

    return true;
}

Value ExpressionDateFromParts::evaluate(const Document& root) const {
    int hour, minute, second, milliseconds;

    if (!evaluateNumberWithinRange(root, _hour, "hour"_sd, 0, 0, 24, &hour) ||
        !evaluateNumberWithinRange(root, _minute, "minute"_sd, 0, 0, 59, &minute) ||
        !evaluateNumberWithinRange(root, _second, "second"_sd, 0, 0, 59, &second) ||
        !evaluateNumberWithinRange(
            root, _milliseconds, "milliseconds"_sd, 0, 0, 999, &milliseconds)) {
        return Value(BSONNULL);
    }

    auto timeZone = makeTimeZone(
        TimeZoneDatabase::get(getExpressionContext()->opCtx->getServiceContext()), root, _timeZone);

    if (!timeZone) {
        return Value(BSONNULL);
    }

    if (_year) {
        int year, month, day;

        if (!evaluateNumberWithinRange(root, _year, "year"_sd, 1970, 0, 9999, &year) ||
            !evaluateNumberWithinRange(root, _month, "month"_sd, 1, 1, 12, &month) ||
            !evaluateNumberWithinRange(root, _day, "day"_sd, 1, 1, 31, &day)) {
            return Value(BSONNULL);
        }

        return Value(
            timeZone->createFromDateParts(year, month, day, hour, minute, second, milliseconds));
    }

    if (_isoYear) {
        int isoYear, isoWeekYear, isoDayOfWeek;

        if (!evaluateNumberWithinRange(root, _isoYear, "isoYear"_sd, 1970, 0, 9999, &isoYear) ||
            !evaluateNumberWithinRange(
                root, _isoWeekYear, "isoWeekYear"_sd, 1, 1, 53, &isoWeekYear) ||
            !evaluateNumberWithinRange(
                root, _isoDayOfWeek, "isoDayOfWeek"_sd, 1, 1, 7, &isoDayOfWeek)) {
            return Value(BSONNULL);
        }

        return Value(timeZone->createFromIso8601DateParts(
            isoYear, isoWeekYear, isoDayOfWeek, hour, minute, second, milliseconds));
    }

    MONGO_UNREACHABLE;
}

void ExpressionDateFromParts::addDependencies(DepsTracker* deps) const {
    if (_year) {
        _year->addDependencies(deps);
    }
    if (_month) {
        _month->addDependencies(deps);
    }
    if (_day) {
        _day->addDependencies(deps);
    }
    if (_hour) {
        _hour->addDependencies(deps);
    }
    if (_minute) {
        _minute->addDependencies(deps);
    }
    if (_second) {
        _second->addDependencies(deps);
    }
    if (_milliseconds) {
        _milliseconds->addDependencies(deps);
    }
    if (_isoYear) {
        _isoYear->addDependencies(deps);
    }
    if (_isoWeekYear) {
        _isoWeekYear->addDependencies(deps);
    }
    if (_isoDayOfWeek) {
        _isoDayOfWeek->addDependencies(deps);
    }
    if (_timeZone) {
        _timeZone->addDependencies(deps);
    }
}

/* ---------------------- ExpressionDateFromString --------------------- */

REGISTER_EXPRESSION(dateFromString, ExpressionDateFromString::parse);
intrusive_ptr<Expression> ExpressionDateFromString::parse(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    BSONElement expr,
    const VariablesParseState& vps) {

    uassert(40540,
            str::stream() << "$dateFromString only supports an object as an argument, found: "
                          << typeName(expr.type()),
            expr.type() == BSONType::Object);

    BSONElement dateStringElem;
    BSONElement timeZoneElem;

    const BSONObj args = expr.embeddedObject();
    for (auto&& arg : args) {
        auto field = arg.fieldNameStringData();

        if (field == "dateString"_sd) {
            dateStringElem = arg;
        } else if (field == "timezone"_sd) {
            timeZoneElem = arg;
        } else {
            uasserted(40541,
                      str::stream() << "Unrecognized argument to $dateFromString: "
                                    << arg.fieldName());
        }
    }

    uassert(40542, "Missing 'dateString' parameter to $dateFromString", dateStringElem);

    return new ExpressionDateFromString(expCtx,
                                        parseOperand(expCtx, dateStringElem, vps),
                                        timeZoneElem ? parseOperand(expCtx, timeZoneElem, vps)
                                                     : nullptr);
}

ExpressionDateFromString::ExpressionDateFromString(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    intrusive_ptr<Expression> dateString,
    intrusive_ptr<Expression> timeZone)
    : Expression(expCtx), _dateString(dateString), _timeZone(timeZone) {}

intrusive_ptr<Expression> ExpressionDateFromString::optimize() {
    _dateString = _dateString->optimize();
    if (_timeZone) {
        _timeZone = _timeZone->optimize();
    }

    if (ExpressionConstant::allNullOrConstant({_dateString, _timeZone})) {
        // Everything is a constant, so we can turn into a constant.
        return ExpressionConstant::create(getExpressionContext(), evaluate(Document{}));
    }
    return this;
}

Value ExpressionDateFromString::serialize(bool explain) const {
    return Value(
        Document{{"$dateFromString",
                  Document{{"dateString", _dateString->serialize(explain)},
                           {"timezone", _timeZone ? _timeZone->serialize(explain) : Value()}}}});
}

Value ExpressionDateFromString::evaluate(const Document& root) const {
    const Value dateString = _dateString->evaluate(root);

    auto timeZone = makeTimeZone(
        TimeZoneDatabase::get(getExpressionContext()->opCtx->getServiceContext()), root, _timeZone);

    if (!timeZone || dateString.nullish()) {
        return Value(BSONNULL);
    }

    uassert(40543,
            str::stream() << "$dateFromString requires that 'dateString' be a string, found: "
                          << typeName(dateString.getType())
                          << " with value "
                          << dateString.toString(),
            dateString.getType() == BSONType::String);
    const std::string& dateTimeString = dateString.getString();

    auto tzdb = TimeZoneDatabase::get(getExpressionContext()->opCtx->getServiceContext());

    return Value(tzdb->fromString(dateTimeString, timeZone));
}

void ExpressionDateFromString::addDependencies(DepsTracker* deps) const {
    _dateString->addDependencies(deps);
    if (_timeZone) {
        _timeZone->addDependencies(deps);
    }
}

/* ---------------------- ExpressionDateToParts ----------------------- */

REGISTER_EXPRESSION(dateToParts, ExpressionDateToParts::parse);
intrusive_ptr<Expression> ExpressionDateToParts::parse(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    BSONElement expr,
    const VariablesParseState& vps) {

    uassert(40524,
            "$dateToParts only supports an object as its argument",
            expr.type() == BSONType::Object);

    BSONElement dateElem;
    BSONElement timeZoneElem;
    BSONElement isoDateElem;

    const BSONObj args = expr.embeddedObject();
    for (auto&& arg : args) {
        auto field = arg.fieldNameStringData();

        if (field == "date"_sd) {
            dateElem = arg;
        } else if (field == "timezone"_sd) {
            timeZoneElem = arg;
        } else if (field == "iso8601"_sd) {
            isoDateElem = arg;
        } else {
            uasserted(40520,
                      str::stream() << "Unrecognized argument to $dateToParts: "
                                    << arg.fieldName());
        }
    }

    uassert(40522, "Missing 'date' parameter to $dateToParts", dateElem);

    return new ExpressionDateToParts(
        expCtx,
        parseOperand(expCtx, dateElem, vps),
        timeZoneElem ? parseOperand(expCtx, timeZoneElem, vps) : nullptr,
        isoDateElem ? parseOperand(expCtx, isoDateElem, vps) : nullptr);
}

ExpressionDateToParts::ExpressionDateToParts(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                             intrusive_ptr<Expression> date,
                                             intrusive_ptr<Expression> timeZone,
                                             intrusive_ptr<Expression> iso8601)
    : Expression(expCtx), _date(date), _timeZone(timeZone), _iso8601(iso8601) {}

intrusive_ptr<Expression> ExpressionDateToParts::optimize() {
    _date = _date->optimize();
    if (_timeZone) {
        _timeZone = _timeZone->optimize();
    }
    if (_iso8601) {
        _iso8601 = _iso8601->optimize();
    }

    if (ExpressionConstant::allNullOrConstant({_date, _iso8601, _timeZone})) {
        // Everything is a constant, so we can turn into a constant.
        return ExpressionConstant::create(getExpressionContext(), evaluate(Document{}));
    }

    return this;
}

Value ExpressionDateToParts::serialize(bool explain) const {
    return Value(
        Document{{"$dateToParts",
                  Document{{"date", _date->serialize(explain)},
                           {"timezone", _timeZone ? _timeZone->serialize(explain) : Value()},
                           {"iso8601", _iso8601 ? _iso8601->serialize(explain) : Value()}}}});
}

boost::optional<int> ExpressionDateToParts::evaluateIso8601Flag(const Document& root) const {
    if (!_iso8601) {
        return false;
    }

    auto iso8601Output = _iso8601->evaluate(root);

    if (iso8601Output.nullish()) {
        return boost::none;
    }

    uassert(40521,
            str::stream() << "iso8601 must evaluate to a bool, found "
                          << typeName(iso8601Output.getType()),
            iso8601Output.getType() == BSONType::Bool);

    return iso8601Output.getBool();
}

Value ExpressionDateToParts::evaluate(const Document& root) const {
    const Value date = _date->evaluate(root);

    auto timeZone = makeTimeZone(
        TimeZoneDatabase::get(getExpressionContext()->opCtx->getServiceContext()), root, _timeZone);
    if (!timeZone) {
        return Value(BSONNULL);
    }

    auto iso8601 = evaluateIso8601Flag(root);
    if (!iso8601) {
        return Value(BSONNULL);
    }

    if (date.nullish()) {
        return Value(BSONNULL);
    }

    auto dateValue = date.coerceToDate();

    if (*iso8601) {
        auto parts = timeZone->dateIso8601Parts(dateValue);
        return Value(Document{{"isoYear", parts.year},
                              {"isoWeekYear", parts.weekOfYear},
                              {"isoDayOfWeek", parts.dayOfWeek},
                              {"hour", parts.hour},
                              {"minute", parts.minute},
                              {"second", parts.second},
                              {"millisecond", parts.millisecond}});
    } else {
        auto parts = timeZone->dateParts(dateValue);
        return Value(Document{{"year", parts.year},
                              {"month", parts.month},
                              {"day", parts.dayOfMonth},
                              {"hour", parts.hour},
                              {"minute", parts.minute},
                              {"second", parts.second},
                              {"millisecond", parts.millisecond}});
    }
}

void ExpressionDateToParts::addDependencies(DepsTracker* deps) const {
    _date->addDependencies(deps);
    if (_timeZone) {
        _timeZone->addDependencies(deps);
    }
    if (_iso8601) {
        _iso8601->addDependencies(deps);
    }
}


/* ---------------------- ExpressionDateToString ----------------------- */

REGISTER_EXPRESSION(dateToString, ExpressionDateToString::parse);
intrusive_ptr<Expression> ExpressionDateToString::parse(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    BSONElement expr,
    const VariablesParseState& vps) {
    verify(str::equals(expr.fieldName(), "$dateToString"));

    uassert(18629, "$dateToString only supports an object as its argument", expr.type() == Object);

    BSONElement formatElem;
    BSONElement dateElem;
    BSONElement timeZoneElem;
    const BSONObj args = expr.embeddedObject();
    BSONForEach(arg, args) {
        if (str::equals(arg.fieldName(), "format")) {
            formatElem = arg;
        } else if (str::equals(arg.fieldName(), "date")) {
            dateElem = arg;
        } else if (str::equals(arg.fieldName(), "timezone")) {
            timeZoneElem = arg;
        } else {
            uasserted(18534,
                      str::stream() << "Unrecognized argument to $dateToString: "
                                    << arg.fieldName());
        }
    }

    uassert(18627, "Missing 'format' parameter to $dateToString", !formatElem.eoo());
    uassert(18628, "Missing 'date' parameter to $dateToString", !dateElem.eoo());

    uassert(18533,
            "The 'format' parameter to $dateToString must be a string literal",
            formatElem.type() == String);

    const string format = formatElem.str();

    TimeZone::validateFormat(format);

    return new ExpressionDateToString(expCtx,
                                      format,
                                      parseOperand(expCtx, dateElem, vps),
                                      timeZoneElem ? parseOperand(expCtx, timeZoneElem, vps)
                                                   : nullptr);
}

ExpressionDateToString::ExpressionDateToString(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const string& format,
    intrusive_ptr<Expression> date,
    intrusive_ptr<Expression> timeZone)
    : Expression(expCtx), _format(format), _date(date), _timeZone(timeZone) {}

intrusive_ptr<Expression> ExpressionDateToString::optimize() {
    _date = _date->optimize();
    if (_timeZone) {
        _timeZone = _timeZone->optimize();
    }

    if (ExpressionConstant::allNullOrConstant({_date, _timeZone})) {
        // Everything is a constant, so we can turn into a constant.
        return ExpressionConstant::create(getExpressionContext(), evaluate(Document{}));
    }

    return this;
}

Value ExpressionDateToString::serialize(bool explain) const {
    return Value(
        Document{{"$dateToString",
                  Document{{"format", _format},
                           {"date", _date->serialize(explain)},
                           {"timezone", _timeZone ? _timeZone->serialize(explain) : Value()}}}});
}

Value ExpressionDateToString::evaluate(const Document& root) const {
    const Value date = _date->evaluate(root);

    auto timeZone = makeTimeZone(
        TimeZoneDatabase::get(getExpressionContext()->opCtx->getServiceContext()), root, _timeZone);
    if (!timeZone) {
        return Value(BSONNULL);
    }

    if (date.nullish()) {
        return Value(BSONNULL);
    }

    return Value(timeZone->formatDate(_format, date.coerceToDate()));
}

void ExpressionDateToString::addDependencies(DepsTracker* deps) const {
    _date->addDependencies(deps);
    if (_timeZone) {
        _timeZone->addDependencies(deps);
    }
}

    void ExpressionDivide::addOperand(
        const intrusive_ptr<Expression> &pExpression) {
        checkArgLimit(2);
        ExpressionNary::addOperand(pExpression);
    }

Value ExpressionDivide::evaluate(const Document& root) const {
    Value lhs = vpOperand[0]->evaluate(root);
    Value rhs = vpOperand[1]->evaluate(root);

        if (lhs.numeric() && rhs.numeric()) {
            double numer = lhs.coerceToDouble();
            double denom = rhs.coerceToDouble();
            uassert(16608, "can't $divide by zero",
                    denom != 0);

            return Value::createDouble(numer / denom);
        }
        else if (lhs.nullish() || rhs.nullish()) {
            return Value(BSONNULL);
        }
        else {
            uasserted(16609, str::stream() << "$divide only supports numeric types, not "
                                           << typeName(lhs.getType())
                                           << " and "
                                           << typeName(rhs.getType()));
        }
    }

    const char *ExpressionDivide::getOpName() const {
        return "$divide";
    }

    /* ---------------------- ExpressionObject --------------------------- */

    ExpressionObject::~ExpressionObject() {
    }

Value ExpressionObject::evaluate(const Document& root) const {
    MutableDocument outputDoc;
    for (auto&& pair : _expressions) {
        outputDoc.addField(pair.first, pair.second->evaluate(root));
    }

    ExpressionObject::ExpressionObject(): _excludeId(false) {
    }

Expression::ComputedPaths ExpressionObject::getComputedPaths(const std::string& exprFieldPath,
                                                             Variables::Id renamingVar) const {
    ComputedPaths outputPaths;
    for (auto&& pair : _expressions) {
        auto exprComputedPaths = pair.second->getComputedPaths(pair.first, renamingVar);
        for (auto&& renames : exprComputedPaths.renames) {
            auto newPath = FieldPath::getFullyQualifiedPath(exprFieldPath, renames.first);
            outputPaths.renames[std::move(newPath)] = renames.second;
        }
        for (auto&& path : exprComputedPaths.paths) {
            outputPaths.paths.insert(FieldPath::getFullyQualifiedPath(exprFieldPath, path));
        }
    }

    return outputPaths;
}

/* --------------------- ExpressionFieldPath --------------------------- */

// this is the old deprecated version only used by tests not using variables
intrusive_ptr<ExpressionFieldPath> ExpressionFieldPath::create(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, const string& fieldPath) {
    return new ExpressionFieldPath(expCtx, "CURRENT." + fieldPath, Variables::kRootId);
}

    bool ExpressionObject::isSimple() {
        for (ExpressionMap::iterator it(_expressions.begin()); it!=_expressions.end(); ++it) {
            if (it->second && !it->second->isSimple())
                return false;
        }
        return true;
    }

    void ExpressionObject::addDependencies(set<string>& deps, vector<string>* path) const {
        string pathStr;
        if (path) {
            if (path->empty()) {
                // we are in the top level of a projection so _id is implicit
                if (!_excludeId)
                    deps.insert("_id");
            }
            else {
                FieldPath f (*path);
                pathStr = f.getPath(false);
                pathStr += '.';
            }
        }
        else {
            verify(!_excludeId);
        }


intrusive_ptr<Expression> ExpressionFieldPath::optimize() {
    if (_variable == Variables::kRemoveId) {
        // The REMOVE system variable optimizes to a constant missing value.
        return ExpressionConstant::create(getExpressionContext(), Value());
    }

    return intrusive_ptr<Expression>(this);
}

void ExpressionFieldPath::addDependencies(DepsTracker* deps) const {
    if (_variable == Variables::kRootId) {  // includes CURRENT when it is equivalent to ROOT.
        if (_fieldPath.getPathLength() == 1) {
            deps->needWholeDocument = true;  // need full doc if just "$$ROOT"
        } else {
            deps->fields.insert(_fieldPath.tail().fullPath());
        }
    }

    void ExpressionObject::addToDocument(
        MutableDocument& out,
        const Document& pDocument,
        const Document& rootDoc
        ) const
    {
        const bool atRoot = (pDocument == rootDoc);

        ExpressionMap::const_iterator end = _expressions.end();

        // This is used to mark fields we've done so that we can add the ones we haven't
        set<string> doneFields;

        FieldIterator fields(pDocument);
        while(fields.more()) {
            Document::FieldPair field (fields.next());

            // TODO don't make a new string here
            const string fieldName = field.first.toString();
            ExpressionMap::const_iterator exprIter = _expressions.find(fieldName);

            // This field is not supposed to be in the output (unless it is _id)
            if (exprIter == end) {
                if (!_excludeId && atRoot && field.first == "_id") {
                    // _id from the root doc is always included (until exclusion is supported)
                    // not updating doneFields since "_id" isn't in _expressions
                    out.addField(field.first, field.second);
                }
                continue;
            }

            // make sure we don't add this field again
            doneFields.insert(exprIter->first);

            Expression* expr = exprIter->second.get();

Value ExpressionFieldPath::evaluate(const Document& root) const {
    auto& vars = getExpressionContext()->variables;
    if (_fieldPath.getPathLength() == 1)  // get the whole variable
        return vars.getValue(_variable, root);

    if (_variable == Variables::kRootId) {
        // ROOT is always a document so use optimized code path
        return evaluatePath(1, root);
    }

    Value var = vars.getValue(_variable, root);
    switch (var.getType()) {
        case Object:
            return evaluatePath(1, var.getDocument());
        case Array:
            return evaluatePathArray(1, var);
        default:
            return Value();
    }
}

            /*
                Check on the type of the input value.  If it's an
                object, just walk down into that recursively, and
                add it to the result.
            */
            if (valueType == Object) {
                MutableDocument sub (exprObj->getSizeHint());
                exprObj->addToDocument(sub, field.second.getDocument(), rootDoc);
                out.addField(field.first, Value(sub.freeze()));
            }
            else if (valueType == Array) {
                /*
                    If it's an array, we have to do the same thing,
                    but to each array element.  Then, add the array
                    of results to the current document.
                */
                vector<Value> result;
                const vector<Value>& input = field.second.getArray();
                for (size_t i=0; i < input.size(); i++) {
                    // can't look for a subfield in a non-object value.
                    if (input[i].getType() != Object)
                        continue;

                    MutableDocument doc (exprObj->getSizeHint());
                    exprObj->addToDocument(doc, input[i].getDocument(), rootDoc);
                    result.push_back(Value(doc.freeze()));
                }

Expression::ComputedPaths ExpressionFieldPath::getComputedPaths(const std::string& exprFieldPath,
                                                                Variables::Id renamingVar) const {
    // An expression field path is either considered a rename or a computed path. We need to find
    // out which case we fall into.
    //
    // The caller has told us that renames must have 'varId' as the first component. We also check
    // that there is only one additional component---no dotted field paths are allowed!  This is
    // because dotted ExpressionFieldPaths can actually reshape the document rather than just
    // changing the field names. This can happen only if there are arrays along the dotted path.
    //
    // For example, suppose you have document {a: [{b: 1}, {b: 2}]}. The projection {"c.d": "$a.b"}
    // does *not* perform the strict rename to yield document {c: [{d: 1}, {d: 2}]}. Instead, it
    // results in the document {c: {d: [1, 2]}}. Due to this reshaping, matches expressed over "a.b"
    // before the $project is applied may not have the same behavior when expressed over "c.d" after
    // the $project is applied.
    ComputedPaths outputPaths;
    if (_variable == renamingVar && _fieldPath.getPathLength() == 2u) {
        outputPaths.renames[exprFieldPath] = _fieldPath.tail().fullPath();
    } else {
        outputPaths.paths.insert(exprFieldPath);
    }

    return outputPaths;
}

/* ------------------------- ExpressionFilter ----------------------------- */

REGISTER_EXPRESSION(filter, ExpressionFilter::parse);
intrusive_ptr<Expression> ExpressionFilter::parse(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    BSONElement expr,
    const VariablesParseState& vpsIn) {
    verify(str::equals(expr.fieldName(), "$filter"));

    uassert(28646, "$filter only supports an object as its argument", expr.type() == Object);

    // "cond" must be parsed after "as" regardless of BSON order.
    BSONElement inputElem;
    BSONElement asElem;
    BSONElement condElem;
    for (auto elem : expr.Obj()) {
        if (str::equals(elem.fieldName(), "input")) {
            inputElem = elem;
        } else if (str::equals(elem.fieldName(), "as")) {
            asElem = elem;
        } else if (str::equals(elem.fieldName(), "cond")) {
            condElem = elem;
        } else {
            uasserted(28647,
                      str::stream() << "Unrecognized parameter to $filter: " << elem.fieldName());
        }

        if (doneFields.size() == _expressions.size())
            return;

        /* add any remaining fields we haven't already taken care of */
        for (vector<string>::const_iterator i(_order.begin()); i!=_order.end(); ++i) {
            ExpressionMap::const_iterator it = _expressions.find(*i);
            string fieldName(it->first);

            /* if we've already dealt with this field, above, do nothing */
            if (doneFields.count(fieldName))
                continue;

            // this is a missing inclusion field
            if (!it->second)
                continue;

            Value pValue(it->second->evaluate(rootDoc));

            /*
              Don't add non-existent values (note:  different from NULL or Undefined);
              this is consistent with existing selection syntax which doesn't
              force the appearnance of non-existent fields.
            */
            if (pValue.missing())
                continue;

            // don't add field if nothing was found in the subobject
            if (dynamic_cast<ExpressionObject*>(it->second.get())
                    && pValue.getDocument()->getFieldCount() == 0)
                continue;


            out.addField(fieldName, pValue);
        }
    }

    size_t ExpressionObject::getSizeHint() const {
        // Note: this can overestimate, but that is better than underestimating
        return _expressions.size() + (_excludeId ? 0 : 1);
    }

Value ExpressionFilter::evaluate(const Document& root) const {
    // We are guaranteed at parse time that this isn't using our _varId.
    const Value inputVal = _input->evaluate(root);
    if (inputVal.nullish())
        return Value(BSONNULL);

    Value ExpressionObject::evaluate(const Document& pDocument) const {
        return Value::createDocument(evaluateDocument(pDocument));
    }

    void ExpressionObject::addField(const FieldPath &fieldPath,
                                    const intrusive_ptr<Expression> &pExpression) {
        const string fieldPart = fieldPath.getFieldName(0);
        const bool haveExpr = _expressions.count(fieldPart);

        intrusive_ptr<Expression>& expr = _expressions[fieldPart]; // inserts if !haveExpr
        intrusive_ptr<ExpressionObject> subObj = dynamic_cast<ExpressionObject*>(expr.get());

    vector<Value> output;
    auto& vars = getExpressionContext()->variables;
    for (const auto& elem : input) {
        vars.setValue(_varId, elem);

        if (_filter->evaluate(root).coerceToBool()) {
            output.push_back(std::move(elem));
        }

        if (!haveExpr)
            expr = subObj = ExpressionObject::create();

        subObj->addField(fieldPath.tail(), pExpression);
    }

    void ExpressionObject::includePath(const string &theFieldPath) {
        addField(theFieldPath, NULL);
    }

    void ExpressionObject::documentToBson(BSONObjBuilder *pBuilder, bool requireExpression) const {
        if (_excludeId)
            pBuilder->appendBool("_id", false);

        for (vector<string>::const_iterator it(_order.begin()); it!=_order.end(); ++it) {
            string fieldName = *it;
            verify(_expressions.find(fieldName) != _expressions.end());
            intrusive_ptr<Expression> expr = _expressions.find(fieldName)->second;

            if (!expr) {
                // this is inclusion, not an expression
                pBuilder->appendBool(fieldName, true);
            }
            else {
                expr->addToBsonObj(pBuilder, fieldName, requireExpression);
            }
        }
    }

    void ExpressionObject::addToBsonObj(BSONObjBuilder *pBuilder,
                                        StringData fieldName,
                                        bool requireExpression) const {

        BSONObjBuilder objBuilder (pBuilder->subobjStart(fieldName));
        documentToBson(&objBuilder, requireExpression);
        objBuilder.done();
    }

    void ExpressionObject::addToBsonArray(
        BSONArrayBuilder *pBuilder) const {

        BSONObjBuilder objBuilder (pBuilder->subobjStart());
        documentToBson(&objBuilder, false);
        objBuilder.done();
    }

    /* --------------------- ExpressionFieldPath --------------------------- */

    ExpressionFieldPath::~ExpressionFieldPath() {
    }

    intrusive_ptr<ExpressionFieldPath> ExpressionFieldPath::create(
        const string &fieldPath) {
        intrusive_ptr<ExpressionFieldPath> pExpression(
            new ExpressionFieldPath(fieldPath));
        return pExpression;
    }

    ExpressionFieldPath::ExpressionFieldPath(
        const string &theFieldPath):
        fieldPath(theFieldPath) {
    }

    intrusive_ptr<Expression> ExpressionFieldPath::optimize() {
        /* nothing can be done for these */
        return intrusive_ptr<Expression>(this);
    }

    void ExpressionFieldPath::addDependencies(set<string>& deps, vector<string>* path) const {
        deps.insert(fieldPath.getPath(false));
    }

    Value ExpressionFieldPath::evaluatePathArray(size_t index, const Value& input) const {
        dassert(input.getType() == Array);

Value ExpressionLet::evaluate(const Document& root) const {
    for (const auto& item : _variables) {
        // It is guaranteed at parse-time that these expressions don't use the variable ids we
        // are setting
        getExpressionContext()->variables.setValue(item.first,
                                                   item.second.expression->evaluate(root));
    }

    return _subExpression->evaluate(root);
}

        return Value::createArray(result);
    }
    Value ExpressionFieldPath::evaluatePath(size_t index, const Document& input) const {
        // Note this function is very hot so it is important that is is well optimized.
        // In particular, all return paths should support RVO.

        /* if we've hit the end of the path, stop */
        if (index == fieldPath.getPathLength() - 1)
            return input[fieldPath.getFieldName(index)];

        // Try to dive deeper
        const Value val = input[fieldPath.getFieldName(index)];
        switch (val.getType()) {
        case Object:
            return evaluatePath(index+1, val.getDocument());

        case Array:
            return evaluatePathArray(index+1, val);

        default:
            return Value();
        }
    }

    Value ExpressionFieldPath::evaluate(const Document& pDocument) const {
        return evaluatePath(0, pDocument);
    }

    void ExpressionFieldPath::addToBsonObj(BSONObjBuilder *pBuilder,
                                           StringData fieldName,
                                           bool requireExpression) const {
        pBuilder->append(fieldName, fieldPath.getPath(true));
    }

    void ExpressionFieldPath::addToBsonArray(
        BSONArrayBuilder *pBuilder) const {
        pBuilder->append(getFieldPath(true));
    }

    /* --------------------- ExpressionFieldRange -------------------------- */

    ExpressionFieldRange::~ExpressionFieldRange() {
    }

    intrusive_ptr<Expression> ExpressionFieldRange::optimize() {
        /* if there is no range to match, this will never evaluate true */
        if (!pRange.get())
            return ExpressionConstant::create(Value(false));

        /*
          If we ended up with a double un-ended range, anything matches.  I
          don't know how that can happen, given intersect()'s interface, but
          here it is, just in case.
        */
        if (pRange->pBottom.missing() && pRange->pTop.missing())
            return ExpressionConstant::create(Value(true));

        /*
          In all other cases, we have to test candidate values.  The
          intersect() method has already optimized those tests, so there
          aren't any more optimizations to look for here.
        */
        return intrusive_ptr<Expression>(this);
    }

    void ExpressionFieldRange::addDependencies(set<string>& deps, vector<string>* path) const {
        pFieldPath->addDependencies(deps);
    }

Value ExpressionMap::evaluate(const Document& root) const {
    // guaranteed at parse time that this isn't using our _varId
    const Value inputVal = _input->evaluate(root);
    if (inputVal.nullish())
        return Value(BSONNULL);

        /* get the value of the specified field */
        Value pValue(pFieldPath->evaluate(pDocument));

        /* see if it fits within any of the ranges */
        if (pRange->contains(pValue))
            return Value(true);

        return Value(false);
    }

    vector<Value> output;
    output.reserve(input.size());
    for (size_t i = 0; i < input.size(); i++) {
        getExpressionContext()->variables.setValue(_varId, input[i]);

        Value toInsert = _each->evaluate(root);
        if (toInsert.missing())
            toInsert = Value(BSONNULL);  // can't insert missing values into array

        // FIXME Append constant values using the $const operator.  SERVER-6769

        // FIXME This checks pointer equality not value equality.
        if (pRange->pTop == pRange->pBottom) {
            BSONArrayBuilder operands;
            pFieldPath->addToBsonArray(&operands);
            pRange->pTop.addToBsonArray(&operands);

            BSONObjBuilder equals;
            equals.append("$eq", operands.arr());
            pBuilder->append(&equals);
            return;
        }

        BSONObjBuilder leftOperator;
        if (!pRange->pBottom.missing()) {
            BSONArrayBuilder leftOperands;
            pFieldPath->addToBsonArray(&leftOperands);
            pRange->pBottom.addToBsonArray(&leftOperands);
            leftOperator.append(
                (pRange->bottomOpen ? "$gt" : "$gte"),
                leftOperands.arr());

            if (pRange->pTop.missing()) {
                pBuilder->append(&leftOperator);
                return;
            }
        }

        BSONObjBuilder rightOperator;
        if (!pRange->pTop.missing()) {
            BSONArrayBuilder rightOperands;
            pFieldPath->addToBsonArray(&rightOperands);
            pRange->pTop.addToBsonArray(&rightOperands);
            rightOperator.append(
                (pRange->topOpen ? "$lt" : "$lte"),
                rightOperands.arr());

            if (pRange->pBottom.missing()) {
                pBuilder->append(&rightOperator);
                return;
            }
        }

Expression::ComputedPaths ExpressionMap::getComputedPaths(const std::string& exprFieldPath,
                                                          Variables::Id renamingVar) const {
    auto inputFieldPath = dynamic_cast<ExpressionFieldPath*>(_input.get());
    if (!inputFieldPath) {
        return {{exprFieldPath}, {}};
    }

    auto inputComputedPaths = inputFieldPath->getComputedPaths("", renamingVar);
    if (inputComputedPaths.renames.empty()) {
        return {{exprFieldPath}, {}};
    }
    invariant(inputComputedPaths.renames.size() == 1u);
    auto fieldPathRenameIter = inputComputedPaths.renames.find("");
    invariant(fieldPathRenameIter != inputComputedPaths.renames.end());
    const auto& oldArrayName = fieldPathRenameIter->second;

    auto eachComputedPaths = _each->getComputedPaths(exprFieldPath, _varId);
    if (eachComputedPaths.renames.empty()) {
        return {{exprFieldPath}, {}};
    }

    // Append the name of the array to the beginning of the old field path.
    for (auto&& rename : eachComputedPaths.renames) {
        eachComputedPaths.renames[rename.first] =
            FieldPath::getFullyQualifiedPath(oldArrayName, rename.second);
    }
    return eachComputedPaths;
}

/* ------------------------- ExpressionMeta ----------------------------- */

REGISTER_EXPRESSION(meta, ExpressionMeta::parse);
intrusive_ptr<Expression> ExpressionMeta::parse(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    BSONElement expr,
    const VariablesParseState& vpsIn) {
    uassert(17307, "$meta only supports string arguments", expr.type() == String);
    if (expr.valueStringData() == "textScore") {
        return new ExpressionMeta(expCtx, MetaType::TEXT_SCORE);
    } else if (expr.valueStringData() == "randVal") {
        return new ExpressionMeta(expCtx, MetaType::RAND_VAL);
    } else {
        uasserted(17308, "Unsupported argument to $meta: " + expr.String());
    }

    void ExpressionFieldRange::addToBsonObj(BSONObjBuilder *pBuilder,
                                            StringData fieldName,
                                            bool requireExpression) const {
        BuilderObj builder(pBuilder, fieldName);
        addToBson(&builder);
    }

Value ExpressionMeta::evaluate(const Document& root) const {
    switch (_metaType) {
        case MetaType::TEXT_SCORE:
            return root.hasTextScore() ? Value(root.getTextScore()) : Value();
        case MetaType::RAND_VAL:
            return root.hasRandMetaField() ? Value(root.getRandMetaField()) : Value();
    }

    void ExpressionFieldRange::toMatcherBson(
        BSONObjBuilder *pBuilder) const {
        verify(pRange.get()); // otherwise, we can't do anything

/* ----------------------- ExpressionMod ---------------------------- */

Value ExpressionMod::evaluate(const Document& root) const {
    Value lhs = vpOperand[0]->evaluate(root);
    Value rhs = vpOperand[1]->evaluate(root);

        /* create the new range */
        scoped_ptr<Range> pNew(new Range(cmpOp, pValue));

        /*
          Go through the range list.  For every range, either add the
          intersection of that to the range list, or if there is none, the
          original range.  This has the effect of restricting overlapping
          ranges, but leaving non-overlapping ones as-is.
        */
        pRange.reset(pRange->intersect(pNew.get()));
    }

    ExpressionFieldRange::Range::Range(CmpOp cmpOp, const Value& pValue):
        bottomOpen(false),
        topOpen(false),
        pBottom(),
        pTop() {
        switch(cmpOp) {

        case EQ:
            pBottom = pTop = pValue;
            break;

        case GT:
            bottomOpen = true;
            /* FALLTHROUGH */
        case GTE:
            topOpen = true;
            pBottom = pValue;
            break;

        case LT:
            topOpen = true;
            /* FALLTHROUGH */
        case LTE:
            bottomOpen = true;
            pTop = pValue;
            break;

        case NE:
        case CMP:
            verify(false); // not allowed
            break;
        }
    }
}

REGISTER_EXPRESSION(mod, ExpressionMod::parse);
const char* ExpressionMod::getOpName() const {
    return "$mod";
}

/* ------------------------- ExpressionMultiply ----------------------------- */

Value ExpressionMultiply::evaluate(const Document& root) const {
    /*
      We'll try to return the narrowest possible result value.  To do that
      without creating intermediate Values, do the arithmetic for double
      and integral types in parallel, tracking the current narrowest
      type.
     */
    double doubleProduct = 1;
    long long longProduct = 1;
    Decimal128 decimalProduct;  // This will be initialized on encountering the first decimal.

    BSONType productType = NumberInt;

    const size_t n = vpOperand.size();
    for (size_t i = 0; i < n; ++i) {
        Value val = vpOperand[i]->evaluate(root);

        if (val.numeric()) {
            BSONType oldProductType = productType;
            productType = Value::getWidestNumeric(productType, val.getType());
            if (productType == NumberDecimal) {
                // On finding the first decimal, convert the partial product to decimal.
                if (oldProductType != NumberDecimal) {
                    decimalProduct = oldProductType == NumberDouble
                        ? Decimal128(doubleProduct, Decimal128::kRoundTo15Digits)
                        : Decimal128(static_cast<int64_t>(longProduct));
                }
            }
        }

        /*
          Find the minimum of the tops of the ranges.

          Start by assuming the minimum is from pRange.  Then, if we have
          values of our own, see if they are less.
        */
        Value pMinTop(pRange->pTop);
        bool minTopOpen = pRange->topOpen;
        if (!pTop.missing()) {
            if (pRange->pTop.missing()) {
                pMinTop = pTop;
                minTopOpen = topOpen;
            }
            else {
                const int cmp = Value::compare(pTop, pRange->pTop);
                if (cmp == 0)
                    minTopOpen = topOpen || pRange->topOpen;
                else if (cmp < 0) {
                    pMinTop = pTop;
                    minTopOpen = topOpen;
                }
            }
        }

        /*
          If the intersections didn't create a disjoint set, create the
          new range.
        */
        if (Value::compare(pMaxBottom, pMinTop) <= 0)
            return new Range(pMaxBottom, maxBottomOpen, pMinTop, minTopOpen);

        /* if we got here, the intersection is empty */
        return NULL;
    }

/* ----------------------- ExpressionIfNull ---------------------------- */

Value ExpressionIfNull::evaluate(const Document& root) const {
    Value pLeft(vpOperand[0]->evaluate(root));
    if (!pLeft.nullish())
        return pLeft;

    Value pRight(vpOperand[1]->evaluate(root));
    return pRight;
}

    ExpressionMillisecond::ExpressionMillisecond():
        ExpressionNary() {
    }

    void ExpressionMillisecond::addOperand(const intrusive_ptr<Expression>& pExpression) {
        checkArgLimit(1);
        ExpressionNary::addOperand(pExpression);
    }

Value ExpressionIn::evaluate(const Document& root) const {
    Value argument(vpOperand[0]->evaluate(root));
    Value arrayOfValues(vpOperand[1]->evaluate(root));

    const char *ExpressionMillisecond::getOpName() const {
        return "$millisecond";
    }

    /* ------------------------- ExpressionMinute -------------------------- */

    ExpressionMinute::~ExpressionMinute() {
    }

    intrusive_ptr<ExpressionNary> ExpressionMinute::create() {
        intrusive_ptr<ExpressionMinute> pExpression(new ExpressionMinute());
        return pExpression;
    }

Value ExpressionIndexOfArray::evaluate(const Document& root) const {
    Value arrayArg = vpOperand[0]->evaluate(root);

    void ExpressionMinute::addOperand(
        const intrusive_ptr<Expression> &pExpression) {
        checkArgLimit(1);
        ExpressionNary::addOperand(pExpression);
    }

    Value ExpressionMinute::evaluate(const Document& pDocument) const {
        checkArgCount(1);
        Value pDate(vpOperand[0]->evaluate(pDocument));
        tm date = pDate.coerceToTm();
        return Value::createInt(date.tm_min);
    }

    const char *ExpressionMinute::getOpName() const {
        return "$minute";
    }

    Value searchItem = vpOperand[1]->evaluate(root);

    size_t startIndex = 0;
    if (vpOperand.size() > 2) {
        Value startIndexArg = vpOperand[2]->evaluate(root);
        uassertIfNotIntegralAndNonNegative(startIndexArg, getOpName(), "starting index");
        startIndex = static_cast<size_t>(startIndexArg.coerceToInt());
    }

    size_t endIndex = array.size();
    if (vpOperand.size() > 3) {
        Value endIndexArg = vpOperand[3]->evaluate(root);
        uassertIfNotIntegralAndNonNegative(endIndexArg, getOpName(), "ending index");
        // Don't let 'endIndex' exceed the length of the array.
        endIndex = std::min(array.size(), static_cast<size_t>(endIndexArg.coerceToInt()));
    }

    ExpressionMod::ExpressionMod():
        ExpressionNary() {
    }

    void ExpressionMod::addOperand(
        const intrusive_ptr<Expression> &pExpression) {
        checkArgLimit(2);
        ExpressionNary::addOperand(pExpression);
    }

    Value ExpressionMod::evaluate(const Document& pDocument) const {
        checkArgCount(2);
        Value lhs = vpOperand[0]->evaluate(pDocument);
        Value rhs = vpOperand[1]->evaluate(pDocument);

        BSONType leftType = lhs.getType();
        BSONType rightType = rhs.getType();

        if (lhs.numeric() && rhs.numeric()) {
            // ensure we aren't modding by 0
            double right = rhs.coerceToDouble();

            uassert(16610, "can't $mod by 0",
                    right != 0);

            if (leftType == NumberDouble
                || (rightType == NumberDouble && rhs.coerceToInt() != right)) {
                // the shell converts ints to doubles so if right is larger than int max or
                // if right truncates to something other than itself, it is a real double.
                // Integer-valued double case is handled below

Value ExpressionIndexOfBytes::evaluate(const Document& root) const {
    Value stringArg = vpOperand[0]->evaluate(root);

            // lastly they must both be ints, return int
            int left = lhs.coerceToInt();
            int rightInt = rhs.coerceToInt();
            return Value::createInt(left % rightInt);
        }
        else if (lhs.nullish() || rhs.nullish()) {
            return Value(BSONNULL);
        }
        else {
            uasserted(16611, str::stream() << "$mod only supports numeric types, not "
                                           << typeName(lhs.getType())
                                           << " and "
                                           << typeName(rhs.getType()));
        }
    }

    const char *ExpressionMod::getOpName() const {
        return "$mod";
    }

    Value tokenArg = vpOperand[1]->evaluate(root);
    uassert(40092,
            str::stream() << "$indexOfBytes requires a string as the second argument, found: "
                          << typeName(tokenArg.getType()),
            tokenArg.getType() == String);
    const std::string& token = tokenArg.getString();

    size_t startIndex = 0;
    if (vpOperand.size() > 2) {
        Value startIndexArg = vpOperand[2]->evaluate(root);
        uassertIfNotIntegralAndNonNegative(startIndexArg, getOpName(), "starting index");
        startIndex = static_cast<size_t>(startIndexArg.coerceToInt());
    }

    size_t endIndex = input.size();
    if (vpOperand.size() > 3) {
        Value endIndexArg = vpOperand[3]->evaluate(root);
        uassertIfNotIntegralAndNonNegative(endIndexArg, getOpName(), "ending index");
        // Don't let 'endIndex' exceed the length of the string.
        endIndex = std::min(input.size(), static_cast<size_t>(endIndexArg.coerceToInt()));
    }

    ExpressionMonth::ExpressionMonth():
        ExpressionNary() {
    }

    void ExpressionMonth::addOperand(const intrusive_ptr<Expression> &pExpression) {
        checkArgLimit(1);
        ExpressionNary::addOperand(pExpression);
    }

    Value ExpressionMonth::evaluate(const Document& pDocument) const {
        checkArgCount(1);
        Value pDate(vpOperand[0]->evaluate(pDocument));
        tm date = pDate.coerceToTm();
        return Value::createInt(date.tm_mon + 1); // MySQL uses 1-12 tm uses 0-11
    }

    const char *ExpressionMonth::getOpName() const {
        return "$month";
    }

    /* ------------------------- ExpressionMultiply ----------------------------- */

    ExpressionMultiply::~ExpressionMultiply() {
    }

Value ExpressionIndexOfCP::evaluate(const Document& root) const {
    Value stringArg = vpOperand[0]->evaluate(root);

    ExpressionMultiply::ExpressionMultiply():
        ExpressionNary() {
    }

    Value ExpressionMultiply::evaluate(const Document& pDocument) const {
        /*
          We'll try to return the narrowest possible result value.  To do that
          without creating intermediate Values, do the arithmetic for double
          and integral types in parallel, tracking the current narrowest
          type.
         */
        double doubleProduct = 1;
        long long longProduct = 1;
        BSONType productType = NumberInt;

    Value tokenArg = vpOperand[1]->evaluate(root);
    uassert(40094,
            str::stream() << "$indexOfCP requires a string as the second argument, found: "
                          << typeName(tokenArg.getType()),
            tokenArg.getType() == String);
    const std::string& token = tokenArg.getString();

    size_t startCodePointIndex = 0;
    if (vpOperand.size() > 2) {
        Value startIndexArg = vpOperand[2]->evaluate(root);
        uassertIfNotIntegralAndNonNegative(startIndexArg, getOpName(), "starting index");
        startCodePointIndex = static_cast<size_t>(startIndexArg.coerceToInt());
    }

                doubleProduct *= val.coerceToDouble();
                longProduct *= val.coerceToLong();
            }
            else if (val.nullish()) {
                return Value(BSONNULL);
            }
            else {
                uasserted(16555, str::stream() << "$multiply only supports numeric types, not "
                                               << typeName(val.getType()));
            }
        }

        if (productType == NumberDouble)
            return Value::createDouble(doubleProduct);
        else if (productType == NumberLong)
            return Value::createLong(longProduct);
        else if (productType == NumberInt)
            return Value::createIntOrLong(longProduct);
        else
            massert(16418, "$multiply resulted in a non-numeric type", false);
    }

    size_t endCodePointIndex = codePointLength;
    if (vpOperand.size() > 3) {
        Value endIndexArg = vpOperand[3]->evaluate(root);
        uassertIfNotIntegralAndNonNegative(endIndexArg, getOpName(), "ending index");

        // Don't let 'endCodePointIndex' exceed the number of code points in the string.
        endCodePointIndex =
            std::min(codePointLength, static_cast<size_t>(endIndexArg.coerceToInt()));
    }

    intrusive_ptr<ExpressionNary> (*ExpressionMultiply::getFactory() const)() {
    return ExpressionMultiply::create;
    }

    /* ------------------------- ExpressionHour ----------------------------- */

    ExpressionHour::~ExpressionHour() {
    }

    intrusive_ptr<ExpressionNary> ExpressionHour::create() {
        intrusive_ptr<ExpressionHour> pExpression(new ExpressionHour());
        return pExpression;
    }

    ExpressionHour::ExpressionHour():
        ExpressionNary() {
    }

    void ExpressionHour::addOperand(const intrusive_ptr<Expression> &pExpression) {
        checkArgLimit(1);
        ExpressionNary::addOperand(pExpression);
    }

    Value ExpressionHour::evaluate(const Document& pDocument) const {
        checkArgCount(1);
        Value pDate(vpOperand[0]->evaluate(pDocument));
        tm date = pDate.coerceToTm();
        return Value::createInt(date.tm_hour);
    }

    const char *ExpressionHour::getOpName() const {
        return "$hour";
    }

/* ----------------------- ExpressionLog ---------------------------- */

Value ExpressionLog::evaluate(const Document& root) const {
    Value argVal = vpOperand[0]->evaluate(root);
    Value baseVal = vpOperand[1]->evaluate(root);
    if (argVal.nullish() || baseVal.nullish())
        return Value(BSONNULL);

    uassert(28756,
            str::stream() << "$log's argument must be numeric, not " << typeName(argVal.getType()),
            argVal.numeric());
    uassert(28757,
            str::stream() << "$log's base must be numeric, not " << typeName(baseVal.getType()),
            baseVal.numeric());

    if (argVal.getType() == NumberDecimal || baseVal.getType() == NumberDecimal) {
        Decimal128 argDecimal = argVal.coerceToDecimal();
        Decimal128 baseDecimal = baseVal.coerceToDecimal();

        if (argDecimal.isGreater(Decimal128::kNormalizedZero) &&
            baseDecimal.isNotEqual(Decimal128(1)) &&
            baseDecimal.isGreater(Decimal128::kNormalizedZero)) {
            return Value(argDecimal.logarithm(baseDecimal));
        }
        // Fall through for error cases.
    }

    double argDouble = argVal.coerceToDouble();
    double baseDouble = baseVal.coerceToDouble();
    uassert(28758,
            str::stream() << "$log's argument must be a positive number, but is " << argDouble,
            argDouble > 0 || std::isnan(argDouble));
    uassert(28759,
            str::stream() << "$log's base must be a positive number not equal to 1, but is "
                          << baseDouble,
            (baseDouble > 0 && baseDouble != 1) || std::isnan(baseDouble));
    return Value(std::log(argDouble) / std::log(baseDouble));
}

    ExpressionIfNull::~ExpressionIfNull() {
    }

    intrusive_ptr<ExpressionNary> ExpressionIfNull::create() {
        intrusive_ptr<ExpressionIfNull> pExpression(new ExpressionIfNull());
        return pExpression;
    }

    ExpressionIfNull::ExpressionIfNull():
        ExpressionNary() {
    }

    void ExpressionIfNull::addOperand(
        const intrusive_ptr<Expression> &pExpression) {
        checkArgLimit(2);
        ExpressionNary::addOperand(pExpression);
    }

    Value ExpressionIfNull::evaluate(const Document& pDocument) const {
        checkArgCount(2);

        Value pLeft(vpOperand[0]->evaluate(pDocument));
        if (!pLeft.nullish())
            return pLeft;

        Value pRight(vpOperand[1]->evaluate(pDocument));
        return pRight;
    }

    const char *ExpressionIfNull::getOpName() const {
        return "$ifNull";
    }
    // If all the operands are constant expressions, collapse the expression into one constant
    // expression.
    if (constOperandCount == vpOperand.size()) {
        return intrusive_ptr<Expression>(
            ExpressionConstant::create(getExpressionContext(), evaluate(Document())));
    }

    // If the expression is associative, we can collapse all the consecutive constant operands into
    // one by applying the expression to those consecutive constant operands.
    // If the expression is also commutative we can reorganize all the operands so that all of the
    // constant ones are together (arbitrarily at the back) and we can collapse all of them into
    // one.
    if (isAssociative()) {
        ExpressionVector constExpressions;
        ExpressionVector optimizedOperands;
        for (size_t i = 0; i < vpOperand.size();) {
            intrusive_ptr<Expression> operand = vpOperand[i];
            // If the operand is a constant one, add it to the current list of consecutive constant
            // operands.
            if (dynamic_cast<ExpressionConstant*>(operand.get())) {
                constExpressions.push_back(operand);
                ++i;
                continue;
            }

    /* ------------------------ ExpressionNary ----------------------------- */

    ExpressionNary::ExpressionNary():
        vpOperand() {
    }

    intrusive_ptr<Expression> ExpressionNary::optimize() {
        unsigned constCount = 0; // count of constant operands
        unsigned stringCount = 0; // count of constant string operands
        const size_t n = vpOperand.size();
        for(size_t i = 0; i < n; ++i) {
            intrusive_ptr<Expression> pNew(vpOperand[i]->optimize());

            /* subsitute the optimized expression */
            vpOperand[i] = pNew;

            /* check to see if the result was a constant */
            const ExpressionConstant *pConst =
                dynamic_cast<ExpressionConstant *>(pNew.get());
            if (pConst) {
                ++constCount;
                if (pConst->getValue().getType() == String)
                    ++stringCount;
            }
        }

        /*
          If all the operands are constant, we can replace this expression
          with a constant.  We can find the value by evaluating this
          expression over a NULL Document because evaluating the
          ExpressionConstant never refers to the argument Document.
        */
        if (constCount == n) {
            Value pResult(evaluate(Document()));
            intrusive_ptr<Expression> pReplacement(
                ExpressionConstant::create(pResult));
            return pReplacement;
        }

            // If the operand is not a constant nor a same-type expression and the expression is
            // not commutative, evaluate an expression of the same type as the one we are
            // optimizing on the list of consecutive constant operands and use the resulting value
            // as a constant expression operand.
            // If the list of consecutive constant operands has less than 2 operands just place
            // back the operands.
            if (!isCommutative()) {
                if (constExpressions.size() > 1) {
                    ExpressionVector vpOperandSave = std::move(vpOperand);
                    vpOperand = std::move(constExpressions);
                    optimizedOperands.emplace_back(
                        ExpressionConstant::create(getExpressionContext(), evaluate(Document())));
                    vpOperand = std::move(vpOperandSave);
                } else {
                    optimizedOperands.insert(
                        optimizedOperands.end(), constExpressions.begin(), constExpressions.end());
                }
            }
        }

        if (constExpressions.size() > 1) {
            vpOperand = std::move(constExpressions);
            optimizedOperands.emplace_back(
                ExpressionConstant::create(getExpressionContext(), evaluate(Document())));
        } else {
            optimizedOperands.insert(
                optimizedOperands.end(), constExpressions.begin(), constExpressions.end());
        }

        return pNew;
    }

    void ExpressionNary::addDependencies(set<string>& deps, vector<string>* path) const {
        for(ExpressionVector::const_iterator i(vpOperand.begin());
            i != vpOperand.end(); ++i) {
            (*i)->addDependencies(deps);
        }
    }

void ExpressionNary::addOperand(const intrusive_ptr<Expression>& pExpression) {
    vpOperand.push_back(pExpression);
}

Value ExpressionNary::serialize(bool explain) const {
    const size_t nOperand = vpOperand.size();
    vector<Value> array;
    /* build up the array */
    for (size_t i = 0; i < nOperand; i++)
        array.push_back(vpOperand[i]->serialize(explain));

    return Value(DOC(getOpName() << array));
}

/* ------------------------- ExpressionNot ----------------------------- */

Value ExpressionNot::evaluate(const Document& root) const {
    Value pOp(vpOperand[0]->evaluate(root));

    bool b = pOp.coerceToBool();
    return Value(!b);
}

REGISTER_EXPRESSION(not, ExpressionNot::parse);
const char* ExpressionNot::getOpName() const {
    return "$not";
}

/* -------------------------- ExpressionOr ----------------------------- */

Value ExpressionOr::evaluate(const Document& root) const {
    const size_t n = vpOperand.size();
    for (size_t i = 0; i < n; ++i) {
        Value pValue(vpOperand[i]->evaluate(root));
        if (pValue.coerceToBool())
            return Value(true);
    }

    intrusive_ptr<ExpressionNary> (*ExpressionNary::getFactory() const)() {
        return NULL;
    }

    void ExpressionNary::toBson(BSONObjBuilder *pBuilder, const char *pOpName) const {
        const size_t nOperand = vpOperand.size();

        /* build up the array */
        BSONArrayBuilder arrBuilder (pBuilder->subarrayStart(pOpName));
        for(size_t i = 0; i < nOperand; ++i)
            vpOperand[i]->addToBsonArray(&arrBuilder);
        arrBuilder.doneFast();
    }

    void ExpressionNary::addToBsonObj(BSONObjBuilder *pBuilder,
                                      StringData fieldName,
                                      bool requireExpression) const {
        BSONObjBuilder exprBuilder;
        toBson(&exprBuilder, getOpName());
        pBuilder->append(fieldName, exprBuilder.done());
    }

    /*
      Remove the final "false" value, and return the new expression.
    */
    pOr->vpOperand.resize(n - 1);
    return pE;
}

REGISTER_EXPRESSION(or, ExpressionOr::parse);
const char* ExpressionOr::getOpName() const {
    return "$or";
}

/* ----------------------- ExpressionPow ---------------------------- */

intrusive_ptr<Expression> ExpressionPow::create(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, Value base, Value exp) {
    intrusive_ptr<ExpressionPow> expr(new ExpressionPow(expCtx));
    expr->vpOperand.push_back(
        ExpressionConstant::create(expr->getExpressionContext(), std::move(base)));
    expr->vpOperand.push_back(
        ExpressionConstant::create(expr->getExpressionContext(), std::move(exp)));
    return expr;
}

Value ExpressionPow::evaluate(const Document& root) const {
    Value baseVal = vpOperand[0]->evaluate(root);
    Value expVal = vpOperand[1]->evaluate(root);
    if (baseVal.nullish() || expVal.nullish())
        return Value(BSONNULL);

    BSONType baseType = baseVal.getType();
    BSONType expType = expVal.getType();

    uassert(28762,
            str::stream() << "$pow's base must be numeric, not " << typeName(baseType),
            baseVal.numeric());
    uassert(28763,
            str::stream() << "$pow's exponent must be numeric, not " << typeName(expType),
            expVal.numeric());

    auto checkNonZeroAndNeg = [](bool isZeroAndNeg) {
        uassert(28764, "$pow cannot take a base of 0 and a negative exponent", !isZeroAndNeg);
    };

    // If either argument is decimal, return a decimal.
    if (baseType == NumberDecimal || expType == NumberDecimal) {
        Decimal128 baseDecimal = baseVal.coerceToDecimal();
        Decimal128 expDecimal = expVal.coerceToDecimal();
        checkNonZeroAndNeg(baseDecimal.isZero() && expDecimal.isNegative());
        return Value(baseDecimal.power(expDecimal));
    }

    void ExpressionNary::checkArgLimit(unsigned maxArgs) const {
        uassert(15993, str::stream() << getOpName() <<
                " only takes " << maxArgs <<
                " operand" << (maxArgs == 1 ? "" : "s"),
                vpOperand.size() < maxArgs);
    }

    void ExpressionNary::checkArgCount(unsigned reqArgs) const {
        uassert(15997, str::stream() << getOpName() <<
                ":  insufficient operands; " << reqArgs <<
                " required, only got " << vpOperand.size(),
                vpOperand.size() == reqArgs);
    }

    /* ------------------------- ExpressionNot ----------------------------- */

    ExpressionNot::~ExpressionNot() {
    }

    intrusive_ptr<ExpressionNary> ExpressionNot::create() {
        intrusive_ptr<ExpressionNot> pExpression(new ExpressionNot());
        return pExpression;
    }

    ExpressionNot::ExpressionNot():
        ExpressionNary() {
    }

/* ------------------------- ExpressionRange ------------------------------ */

Value ExpressionRange::evaluate(const Document& root) const {
    Value startVal(vpOperand[0]->evaluate(root));
    Value endVal(vpOperand[1]->evaluate(root));

    uassert(34443,
            str::stream() << "$range requires a numeric starting value, found value of type: "
                          << typeName(startVal.getType()),
            startVal.numeric());
    uassert(34444,
            str::stream() << "$range requires a starting value that can be represented as a 32-bit "
                             "integer, found value: "
                          << startVal.toString(),
            startVal.integral());
    uassert(34445,
            str::stream() << "$range requires a numeric ending value, found value of type: "
                          << typeName(endVal.getType()),
            endVal.numeric());
    uassert(34446,
            str::stream() << "$range requires an ending value that can be represented as a 32-bit "
                             "integer, found value: "
                          << endVal.toString(),
            endVal.integral());

    int current = startVal.coerceToInt();
    int end = endVal.coerceToInt();

    int step = 1;
    if (vpOperand.size() == 3) {
        // A step was specified by the user.
        Value stepVal(vpOperand[2]->evaluate(root));

        uassert(34447,
                str::stream() << "$range requires a numeric step value, found value of type:"
                              << typeName(stepVal.getType()),
                stepVal.numeric());
        uassert(34448,
                str::stream() << "$range requires a step value that can be represented as a 32-bit "
                                 "integer, found value: "
                              << stepVal.toString(),
                stepVal.integral());
        step = stepVal.coerceToInt();

        uassert(34449, "$range requires a non-zero step value", step != 0);
    }

    std::vector<Value> output;

    while ((step > 0 ? current < end : current > end)) {
        output.push_back(Value(current));
        current += step;
    }

    return Value(output);
}

REGISTER_EXPRESSION(range, ExpressionRange::parse);
const char* ExpressionRange::getOpName() const {
    return "$range";
}

/* ------------------------ ExpressionReduce ------------------------------ */

REGISTER_EXPRESSION(reduce, ExpressionReduce::parse);
intrusive_ptr<Expression> ExpressionReduce::parse(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    BSONElement expr,
    const VariablesParseState& vps) {
    uassert(40075,
            str::stream() << "$reduce requires an object as an argument, found: "
                          << typeName(expr.type()),
            expr.type() == Object);

    intrusive_ptr<ExpressionReduce> reduce(new ExpressionReduce(expCtx));

    // vpsSub is used only to parse 'in', which must have access to $$this and $$value.
    VariablesParseState vpsSub(vps);
    reduce->_thisVar = vpsSub.defineVariable("this");
    reduce->_valueVar = vpsSub.defineVariable("value");

    for (auto&& elem : expr.Obj()) {
        auto field = elem.fieldNameStringData();

        if (field == "input") {
            reduce->_input = parseOperand(expCtx, elem, vps);
        } else if (field == "initialValue") {
            reduce->_initial = parseOperand(expCtx, elem, vps);
        } else if (field == "in") {
            reduce->_in = parseOperand(expCtx, elem, vpsSub);
        } else {
            uasserted(40076, str::stream() << "$reduce found an unknown argument: " << field);
        }
    }

    uassert(40077, "$reduce requires 'input' to be specified", reduce->_input);
    uassert(40078, "$reduce requires 'initialValue' to be specified", reduce->_initial);
    uassert(40079, "$reduce requires 'in' to be specified", reduce->_in);

    return reduce;
}

Value ExpressionReduce::evaluate(const Document& root) const {
    Value inputVal = _input->evaluate(root);

        bool b = pOp.coerceToBool();
        return Value(!b);
    }

    uassert(40080,
            str::stream() << "$reduce requires that 'input' be an array, found: "
                          << inputVal.toString(),
            inputVal.isArray());

    Value accumulatedValue = _initial->evaluate(root);
    auto& vars = getExpressionContext()->variables;

    for (auto&& elem : inputVal.getArray()) {
        vars.setValue(_thisVar, elem);
        vars.setValue(_valueVar, accumulatedValue);

        accumulatedValue = _in->evaluate(root);
    }

    /* -------------------------- ExpressionOr ----------------------------- */

/* ------------------------ ExpressionReverseArray ------------------------ */

Value ExpressionReverseArray::evaluate(const Document& root) const {
    Value input(vpOperand[0]->evaluate(root));

    if (input.nullish()) {
        return Value(BSONNULL);
    }

    intrusive_ptr<ExpressionNary> ExpressionOr::create() {
        intrusive_ptr<ExpressionNary> pExpression(new ExpressionOr());
        return pExpression;
    }

    std::vector<Value> array = input.getArray();
    std::reverse(array.begin(), array.end());
    return Value(array);
}

REGISTER_EXPRESSION(reverseArray, ExpressionReverseArray::parse);
const char* ExpressionReverseArray::getOpName() const {
    return "$reverseArray";
}

namespace {
ValueSet arrayToSet(const Value& val, const ValueComparator& valueComparator) {
    const vector<Value>& array = val.getArray();
    ValueSet valueSet = valueComparator.makeOrderedValueSet();
    valueSet.insert(array.begin(), array.end());
    return valueSet;
}
}

/* ----------------------- ExpressionSetDifference ---------------------------- */

Value ExpressionSetDifference::evaluate(const Document& root) const {
    const Value lhs = vpOperand[0]->evaluate(root);
    const Value rhs = vpOperand[1]->evaluate(root);

    if (lhs.nullish() || rhs.nullish()) {
        return Value(BSONNULL);
    }

    Value ExpressionOr::evaluate(const Document& pDocument) const {
        const size_t n = vpOperand.size();
        for(size_t i = 0; i < n; ++i) {
            Value pValue(vpOperand[i]->evaluate(pDocument));
            if (pValue.coerceToBool())
                return Value(true);
        }

REGISTER_EXPRESSION(setDifference, ExpressionSetDifference::parse);
const char* ExpressionSetDifference::getOpName() const {
    return "$setDifference";
}

/* ----------------------- ExpressionSetEquals ---------------------------- */

void ExpressionSetEquals::validateArguments(const ExpressionVector& args) const {
    uassert(17045,
            str::stream() << "$setEquals needs at least two arguments had: " << args.size(),
            args.size() >= 2);
}

Value ExpressionSetEquals::evaluate(const Document& root) const {
    const size_t n = vpOperand.size();
    const auto& valueComparator = getExpressionContext()->getValueComparator();
    ValueSet lhs = valueComparator.makeOrderedValueSet();

    for (size_t i = 0; i < n; i++) {
        const Value nextEntry = vpOperand[i]->evaluate(root);
        uassert(17044,
                str::stream() << "All operands of $setEquals must be arrays. One "
                              << "argument is of type: "
                              << typeName(nextEntry.getType()),
                nextEntry.isArray());

        if (i == 0) {
            lhs.insert(nextEntry.getArray().begin(), nextEntry.getArray().end());
        } else {
            ValueSet rhs = valueComparator.makeOrderedValueSet();
            rhs.insert(nextEntry.getArray().begin(), nextEntry.getArray().end());
            if (lhs.size() != rhs.size()) {
                return Value(false);
            }

            if (!std::equal(lhs.begin(), lhs.end(), rhs.begin(), valueComparator.getEqualTo())) {
                return Value(false);
            }
        }

/* ----------------------- ExpressionSetIntersection ---------------------------- */

Value ExpressionSetIntersection::evaluate(const Document& root) const {
    const size_t n = vpOperand.size();
    const auto& valueComparator = getExpressionContext()->getValueComparator();
    ValueSet currentIntersection = valueComparator.makeOrderedValueSet();
    for (size_t i = 0; i < n; i++) {
        const Value nextEntry = vpOperand[i]->evaluate(root);
        if (nextEntry.nullish()) {
            return Value(BSONNULL);
        }

        /*
          Remove the final "false" value, and return the new expression.
        */
        pOr->vpOperand.resize(n - 1);
        return pE;
    }

Value ExpressionSetIsSubset::evaluate(const Document& root) const {
    const Value lhs = vpOperand[0]->evaluate(root);
    const Value rhs = vpOperand[1]->evaluate(root);

    uassert(17046,
            str::stream() << "both operands of $setIsSubset must be arrays. First "
                          << "argument is of type: "
                          << typeName(lhs.getType()),
            lhs.isArray());
    uassert(17042,
            str::stream() << "both operands of $setIsSubset must be arrays. Second "
                          << "argument is of type: "
                          << typeName(rhs.getType()),
            rhs.isArray());

    return setIsSubsetHelper(lhs.getArray(),
                             arrayToSet(rhs, getExpressionContext()->getValueComparator()));
}

/**
 * This class handles the case where the RHS set is constant.
 *
 * Since it is constant we can construct the hashset once which makes the runtime performance
 * effectively constant with respect to the size of RHS. Large, constant RHS is expected to be a
 * major use case for $redact and this has been verified to improve performance significantly.
 */
class ExpressionSetIsSubset::Optimized : public ExpressionSetIsSubset {
public:
    Optimized(const boost::intrusive_ptr<ExpressionContext>& expCtx,
              const ValueSet& cachedRhsSet,
              const ExpressionVector& operands)
        : ExpressionSetIsSubset(expCtx), _cachedRhsSet(cachedRhsSet) {
        vpOperand = operands;
    }

    virtual Value evaluate(const Document& root) const {
        const Value lhs = vpOperand[0]->evaluate(root);

        uassert(17310,
                str::stream() << "both operands of $setIsSubset must be arrays. First "
                              << "argument is of type: "
                              << typeName(lhs.getType()),
                lhs.isArray());

        return setIsSubsetHelper(lhs.getArray(), _cachedRhsSet);
    }

private:
    const ValueSet _cachedRhsSet;
};

intrusive_ptr<Expression> ExpressionSetIsSubset::optimize() {
    // perfore basic optimizations
    intrusive_ptr<Expression> optimized = ExpressionNary::optimize();

    // if ExpressionNary::optimize() created a new value, return it directly
    if (optimized.get() != this)
        return optimized;

    if (ExpressionConstant* ec = dynamic_cast<ExpressionConstant*>(vpOperand[1].get())) {
        const Value rhs = ec->getValue();
        uassert(17311,
                str::stream() << "both operands of $setIsSubset must be arrays. Second "
                              << "argument is of type: "
                              << typeName(rhs.getType()),
                rhs.isArray());

        intrusive_ptr<Expression> optimizedWithConstant(
            new Optimized(this->getExpressionContext(),
                          arrayToSet(rhs, getExpressionContext()->getValueComparator()),
                          vpOperand));
        return optimizedWithConstant;
    }
    return optimized;
}

    ExpressionSecond::~ExpressionSecond() {
    }

    intrusive_ptr<ExpressionNary> ExpressionSecond::create() {
        intrusive_ptr<ExpressionSecond> pExpression(new ExpressionSecond());
        return pExpression;
    }

Value ExpressionSetUnion::evaluate(const Document& root) const {
    ValueSet unionedSet = getExpressionContext()->getValueComparator().makeOrderedValueSet();
    const size_t n = vpOperand.size();
    for (size_t i = 0; i < n; i++) {
        const Value newEntries = vpOperand[i]->evaluate(root);
        if (newEntries.nullish()) {
            return Value(BSONNULL);
        }
        uassert(17043,
                str::stream() << "All operands of $setUnion must be arrays. One argument"
                              << " is of type: "
                              << typeName(newEntries.getType()),
                newEntries.isArray());

    void ExpressionSecond::addOperand(const intrusive_ptr<Expression> &pExpression) {
        checkArgLimit(1);
        ExpressionNary::addOperand(pExpression);
    }

    Value ExpressionSecond::evaluate(const Document& pDocument) const {
        checkArgCount(1);
        Value pDate(vpOperand[0]->evaluate(pDocument));
        tm date = pDate.coerceToTm();
        return Value::createInt(date.tm_sec);
    }

    const char *ExpressionSecond::getOpName() const {
        return "$second";
    }

Value ExpressionIsArray::evaluate(const Document& root) const {
    Value argument = vpOperand[0]->evaluate(root);
    return Value(argument.isArray());
}


/* ----------------------- ExpressionSlice ---------------------------- */

Value ExpressionSlice::evaluate(const Document& root) const {
    const size_t n = vpOperand.size();

    Value arrayVal = vpOperand[0]->evaluate(root);
    // Could be either a start index or the length from 0.
    Value arg2 = vpOperand[1]->evaluate(root);

    if (arrayVal.nullish() || arg2.nullish()) {
        return Value(BSONNULL);
    }

    uassert(28724,
            str::stream() << "First argument to $slice must be an array, but is"
                          << " of type: "
                          << typeName(arrayVal.getType()),
            arrayVal.isArray());
    uassert(28725,
            str::stream() << "Second argument to $slice must be a numeric value,"
                          << " but is of type: "
                          << typeName(arg2.getType()),
            arg2.numeric());
    uassert(28726,
            str::stream() << "Second argument to $slice can't be represented as"
                          << " a 32-bit integer: "
                          << arg2.coerceToDouble(),
            arg2.integral());

    const auto& array = arrayVal.getArray();
    size_t start;
    size_t end;

    if (n == 2) {
        // Only count given.
        int count = arg2.coerceToInt();
        start = 0;
        end = array.size();
        if (count >= 0) {
            end = std::min(end, size_t(count));
        } else {
            // Negative count's start from the back. If a abs(count) is greater
            // than the
            // length of the array, return the whole array.
            start = std::max(0, static_cast<int>(array.size()) + count);
        }
    } else {
        // We have both a start index and a count.
        int startInt = arg2.coerceToInt();
        if (startInt < 0) {
            // Negative values start from the back. If a abs(start) is greater
            // than the length
            // of the array, start from 0.
            start = std::max(0, static_cast<int>(array.size()) + startInt);
        } else {
            start = std::min(array.size(), size_t(startInt));
        }

        Value countVal = vpOperand[2]->evaluate(root);

        intrusive_ptr<ExpressionNary> ExpressionSplit::create() {
            intrusive_ptr<ExpressionSplit> pExpression(new ExpressionSplit());
            return pExpression;
        }

        uassert(28727,
                str::stream() << "Third argument to $slice must be numeric, but "
                              << "is of type: "
                              << typeName(countVal.getType()),
                countVal.numeric());
        uassert(28728,
                str::stream() << "Third argument to $slice can't be represented"
                              << " as a 32-bit integer: "
                              << countVal.coerceToDouble(),
                countVal.integral());
        uassert(28729,
                str::stream() << "Third argument to $slice must be positive: "
                              << countVal.coerceToInt(),
                countVal.coerceToInt() > 0);

        size_t count = size_t(countVal.coerceToInt());
        end = std::min(start + count, array.size());
    }

    return Value(vector<Value>(array.begin() + start, array.begin() + end));
}

REGISTER_EXPRESSION(slice, ExpressionSlice::parse);
const char* ExpressionSlice::getOpName() const {
    return "$slice";
}

/* ----------------------- ExpressionSize ---------------------------- */

Value ExpressionSize::evaluate(const Document& root) const {
    Value array = vpOperand[0]->evaluate(root);

    uassert(17124,
            str::stream() << "The argument to $size must be an array, but was of type: "
                          << typeName(array.getType()),
            array.isArray());
    return Value::createIntOrLong(array.getArray().size());
}

REGISTER_EXPRESSION(size, ExpressionSize::parse);
const char* ExpressionSize::getOpName() const {
    return "$size";
}

/* ----------------------- ExpressionSplit --------------------------- */

Value ExpressionSplit::evaluate(const Document& root) const {
    Value inputArg = vpOperand[0]->evaluate(root);
    Value separatorArg = vpOperand[1]->evaluate(root);

        Value ExpressionSplit::evaluate(const Document& pDocument) const {
            checkArgCount(2);
            Value string1 = vpOperand[0]->evaluate(pDocument);
            Value string2 = vpOperand[1]->evaluate(pDocument);

            string toSplit = string1.coerceToString();
            string pattern = string2.coerceToString();

            string::size_type length = pattern.length();
            string::size_type limit = toSplit.length();
            vector<Value> result;

            if ( pattern != "" && limit >= length ){
                size_t current = 0;
                string::size_type next = -1;
                do{
                    next = toSplit.find(pattern, current);
                    Value toPush = Value::createString(toSplit.substr( current, next - current));
                    result.push_back(toPush);
                    current = next + length;

                }while ( next != string::npos);
            } else {
            	result.push_back(Value::createString(toSplit));
            }

            //case the pattern is not found we return an array with the original value.
            if ( result.size() == 0 ){
            	result.push_back(Value::createString(toSplit));
            }

            return Value::createArray(result);
        }

        const char *ExpressionSplit::getOpName() const {
            return "$split";
        }


    /* ----------------------- ExpressionStrcasecmp ---------------------------- */

    ExpressionStrcasecmp::~ExpressionStrcasecmp() {
    }

Value ExpressionStrcasecmp::evaluate(const Document& root) const {
    Value pString1(vpOperand[0]->evaluate(root));
    Value pString2(vpOperand[1]->evaluate(root));

    /* boost::iequals returns a bool not an int so strings must actually be allocated */
    string str1 = boost::to_upper_copy(pString1.coerceToString());
    string str2 = boost::to_upper_copy(pString2.coerceToString());
    int result = str1.compare(str2);

    if (result == 0)
        return Value(0);
    else if (result > 0)
        return Value(1);
    else
        return Value(-1);
}

REGISTER_EXPRESSION(strcasecmp, ExpressionStrcasecmp::parse);
const char* ExpressionStrcasecmp::getOpName() const {
    return "$strcasecmp";
}

/* ----------------------- ExpressionSubstrBytes ---------------------------- */

Value ExpressionSubstrBytes::evaluate(const Document& root) const {
    Value pString(vpOperand[0]->evaluate(root));
    Value pLower(vpOperand[1]->evaluate(root));
    Value pLength(vpOperand[2]->evaluate(root));

    string str = pString.coerceToString();
    uassert(16034,
            str::stream() << getOpName()
                          << ":  starting index must be a numeric type (is BSON type "
                          << typeName(pLower.getType())
                          << ")",
            (pLower.getType() == NumberInt || pLower.getType() == NumberLong ||
             pLower.getType() == NumberDouble));
    uassert(16035,
            str::stream() << getOpName() << ":  length must be a numeric type (is BSON type "
                          << typeName(pLength.getType())
                          << ")",
            (pLength.getType() == NumberInt || pLength.getType() == NumberLong ||
             pLength.getType() == NumberDouble));

    string::size_type lower = static_cast<string::size_type>(pLower.coerceToLong());
    string::size_type length = static_cast<string::size_type>(pLength.coerceToLong());

    uassert(28656,
            str::stream() << getOpName()
                          << ":  Invalid range, starting index is a UTF-8 continuation byte.",
            (lower >= str.length() || !isContinuationByte(str[lower])));

    // Check the byte after the last character we'd return. If it is a continuation byte, that
    // means we're in the middle of a UTF-8 character.
    uassert(
        28657,
        str::stream() << getOpName()
                      << ":  Invalid range, ending index is in the middle of a UTF-8 character.",
        (lower + length >= str.length() || !isContinuationByte(str[lower + length])));

    if (lower >= str.length()) {
        // If lower > str.length() then string::substr() will throw out_of_range, so return an
        // empty string if lower is not a valid string index.
        return Value(StringData());
    }
    return Value(str.substr(lower, length));
}

// $substr is deprecated in favor of $substrBytes, but for now will just parse into a $substrBytes.
REGISTER_EXPRESSION(substrBytes, ExpressionSubstrBytes::parse);
REGISTER_EXPRESSION(substr, ExpressionSubstrBytes::parse);
const char* ExpressionSubstrBytes::getOpName() const {
    return "$substrBytes";
}

/* ----------------------- ExpressionSubstrCP ---------------------------- */

Value ExpressionSubstrCP::evaluate(const Document& root) const {
    Value inputVal(vpOperand[0]->evaluate(root));
    Value lowerVal(vpOperand[1]->evaluate(root));
    Value lengthVal(vpOperand[2]->evaluate(root));

    std::string str = inputVal.coerceToString();
    uassert(34450,
            str::stream() << getOpName() << ": starting index must be a numeric type (is BSON type "
                          << typeName(lowerVal.getType())
                          << ")",
            lowerVal.numeric());
    uassert(34451,
            str::stream() << getOpName()
                          << ": starting index cannot be represented as a 32-bit integral value: "
                          << lowerVal.toString(),
            lowerVal.integral());
    uassert(34452,
            str::stream() << getOpName() << ": length must be a numeric type (is BSON type "
                          << typeName(lengthVal.getType())
                          << ")",
            lengthVal.numeric());
    uassert(34453,
            str::stream() << getOpName()
                          << ": length cannot be represented as a 32-bit integral value: "
                          << lengthVal.toString(),
            lengthVal.integral());

    int startIndexCodePoints = lowerVal.coerceToInt();
    int length = lengthVal.coerceToInt();

    uassert(34454,
            str::stream() << getOpName() << ": length must be a nonnegative integer.",
            length >= 0);

    uassert(34455,
            str::stream() << getOpName() << ": the starting index must be nonnegative integer.",
            startIndexCodePoints >= 0);

    size_t startIndexBytes = 0;

    for (int i = 0; i < startIndexCodePoints; i++) {
        if (startIndexBytes >= str.size()) {
            return Value(StringData());
        }
        uassert(34456,
                str::stream() << getOpName() << ": invalid UTF-8 string",
                !isContinuationByte(str[startIndexBytes]));
        size_t codePointLength = getCodePointLength(str[startIndexBytes]);
        uassert(
            34457, str::stream() << getOpName() << ": invalid UTF-8 string", codePointLength <= 4);
        startIndexBytes += codePointLength;
    }

    ExpressionStrcasecmp::ExpressionStrcasecmp():
        ExpressionNary() {
    }

    return Value(std::string(str, startIndexBytes, endIndexBytes - startIndexBytes));
}

REGISTER_EXPRESSION(substrCP, ExpressionSubstrCP::parse);
const char* ExpressionSubstrCP::getOpName() const {
    return "$substrCP";
}

/* ----------------------- ExpressionStrLenBytes ------------------------- */

Value ExpressionStrLenBytes::evaluate(const Document& root) const {
    Value str(vpOperand[0]->evaluate(root));

    uassert(34473,
            str::stream() << "$strLenBytes requires a string argument, found: "
                          << typeName(str.getType()),
            str.getType() == String);

    size_t strLen = str.getString().size();

    uassert(34470,
            "string length could not be represented as an int.",
            strLen <= std::numeric_limits<int>::max());
    return Value(static_cast<int>(strLen));
}

REGISTER_EXPRESSION(strLenBytes, ExpressionStrLenBytes::parse);
const char* ExpressionStrLenBytes::getOpName() const {
    return "$strLenBytes";
}

/* ----------------------- ExpressionStrLenCP ------------------------- */

Value ExpressionStrLenCP::evaluate(const Document& root) const {
    Value val(vpOperand[0]->evaluate(root));

        /* boost::iequals returns a bool not an int so strings must actually be allocated */
        string str1 = boost::to_upper_copy( pString1.coerceToString() );
        string str2 = boost::to_upper_copy( pString2.coerceToString() );
        int result = str1.compare(str2);

        if (result == 0)
            return Value(0);
        else if (result > 0)
            return Value(1);
        else
            return Value(-1);
    }

    const char *ExpressionStrcasecmp::getOpName() const {
        return "$strcasecmp";
    }

    /* ----------------------- ExpressionSubstr ---------------------------- */

    ExpressionSubstr::~ExpressionSubstr() {
    }

    intrusive_ptr<ExpressionNary> ExpressionSubstr::create() {
        intrusive_ptr<ExpressionSubstr> pExpression(new ExpressionSubstr());
        return pExpression;
    }

/* ----------------------- ExpressionSubtract ---------------------------- */

Value ExpressionSubtract::evaluate(const Document& root) const {
    Value lhs = vpOperand[0]->evaluate(root);
    Value rhs = vpOperand[1]->evaluate(root);

    BSONType diffType = Value::getWidestNumeric(rhs.getType(), lhs.getType());

    if (diffType == NumberDecimal) {
        Decimal128 right = rhs.coerceToDecimal();
        Decimal128 left = lhs.coerceToDecimal();
        return Value(left.subtract(right));
    } else if (diffType == NumberDouble) {
        double right = rhs.coerceToDouble();
        double left = lhs.coerceToDouble();
        return Value(left - right);
    } else if (diffType == NumberLong) {
        long long right = rhs.coerceToLong();
        long long left = lhs.coerceToLong();
        return Value(left - right);
    } else if (diffType == NumberInt) {
        long long right = rhs.coerceToLong();
        long long left = lhs.coerceToLong();
        return Value::createIntOrLong(left - right);
    } else if (lhs.nullish() || rhs.nullish()) {
        return Value(BSONNULL);
    } else if (lhs.getType() == Date) {
        if (rhs.getType() == Date) {
            return Value(durationCount<Milliseconds>(lhs.getDate() - rhs.getDate()));
        } else if (rhs.numeric()) {
            return Value(lhs.getDate() - Milliseconds(rhs.coerceToLong()));
        } else {
            uasserted(16613,
                      str::stream() << "cant $subtract a " << typeName(rhs.getType())
                                    << " from a Date");
        }
        return Value::createString( str.substr(lower, length) );
    }

    const char *ExpressionSubstr::getOpName() const {
        return "$substr";
    }

Value ExpressionSwitch::evaluate(const Document& root) const {
    for (auto&& branch : _branches) {
        Value caseExpression(branch.first->evaluate(root));

        if (caseExpression.coerceToBool()) {
            return branch.second->evaluate(root);
        }
    }

    intrusive_ptr<ExpressionNary> ExpressionSubtract::create() {
        intrusive_ptr<ExpressionSubtract> pExpression(new ExpressionSubtract());
        return pExpression;
    }

    return _default->evaluate(root);
}

    void ExpressionSubtract::addOperand(
        const intrusive_ptr<Expression> &pExpression) {
        checkArgLimit(2);
        ExpressionNary::addOperand(pExpression);
    }

    Value ExpressionSubtract::evaluate(const Document& pDocument) const {
        checkArgCount(2);
        Value lhs = vpOperand[0]->evaluate(pDocument);
        Value rhs = vpOperand[1]->evaluate(pDocument);

        BSONType diffType = Value::getWidestNumeric(rhs.getType(), lhs.getType());

        if (diffType == NumberDouble) {
            double right = rhs.coerceToDouble();
            double left = lhs.coerceToDouble();
            return Value::createDouble(left - right);
        }
        else if (diffType == NumberLong) {
            long long right = rhs.coerceToLong();
            long long left = lhs.coerceToLong();
            return Value::createLong(left - right);
        }
        else if (diffType == NumberInt) {
            long long right = rhs.coerceToLong();
            long long left = lhs.coerceToLong();
            return Value::createIntOrLong(left - right);
        }
        else if (lhs.nullish() || rhs.nullish()) {
            return Value(BSONNULL);
        }
        else if (lhs.getType() == Date) {
            if (rhs.getType() == Date) {
                long long timeDelta = lhs.getDate() - rhs.getDate();
                return Value(timeDelta);
            }
            else if (rhs.numeric()) {
                long long millisSinceEpoch = lhs.getDate() - rhs.coerceToLong();
                return Value(Date_t(millisSinceEpoch));
            }
            else {
                uasserted(16613, str::stream() << "cant $subtract a "
                                               << typeName(rhs.getType())
                                               << " from a Date");
            }
        }
        else {
            uasserted(16556, str::stream() << "cant $subtract a"
                                           << typeName(rhs.getType())
                                           << " from a "
                                           << typeName(lhs.getType()));
        }
    }

    const char *ExpressionSubtract::getOpName() const {
        return "$subtract";
    }

    /* ------------------------- ExpressionToLower ----------------------------- */

    ExpressionToLower::~ExpressionToLower() {
    }

    intrusive_ptr<ExpressionNary> ExpressionToLower::create() {
        intrusive_ptr<ExpressionToLower> pExpression(new ExpressionToLower());
        return pExpression;
    }

    ExpressionToLower::ExpressionToLower():
        ExpressionNary() {
    }

    void ExpressionToLower::addOperand(const intrusive_ptr<Expression> &pExpression) {
        checkArgLimit(1);
        ExpressionNary::addOperand(pExpression);
    }

    Value ExpressionToLower::evaluate(const Document& pDocument) const {
        checkArgCount(1);
        Value pString(vpOperand[0]->evaluate(pDocument));
        string str = pString.coerceToString();
        boost::to_lower(str);
        return Value::createString(str);
    }

    return Value(Document{{"$switch", Document{{"branches", Value(serializedBranches)}}}});
}

/* ------------------------- ExpressionToLower ----------------------------- */

Value ExpressionToLower::evaluate(const Document& root) const {
    Value pString(vpOperand[0]->evaluate(root));
    string str = pString.coerceToString();
    boost::to_lower(str);
    return Value(str);
}

REGISTER_EXPRESSION(toLower, ExpressionToLower::parse);
const char* ExpressionToLower::getOpName() const {
    return "$toLower";
}

/* ------------------------- ExpressionToUpper -------------------------- */

Value ExpressionToUpper::evaluate(const Document& root) const {
    Value pString(vpOperand[0]->evaluate(root));
    string str(pString.coerceToString());
    boost::to_upper(str);
    return Value(str);
}

REGISTER_EXPRESSION(toUpper, ExpressionToUpper::parse);
const char* ExpressionToUpper::getOpName() const {
    return "$toUpper";
}

/* ------------------------- ExpressionTrunc -------------------------- */

Value ExpressionTrunc::evaluateNumericArg(const Value& numericArg) const {
    // There's no point in truncating integers or longs, it will have no effect.
    switch (numericArg.getType()) {
        case NumberDecimal:
            return Value(numericArg.getDecimal().quantize(Decimal128::kNormalizedZero,
                                                          Decimal128::kRoundTowardZero));
        case NumberDouble:
            return Value(std::trunc(numericArg.getDouble()));
        default:
            return numericArg;
    }
}

REGISTER_EXPRESSION(trunc, ExpressionTrunc::parse);
const char* ExpressionTrunc::getOpName() const {
    return "$trunc";
}

/* ------------------------- ExpressionType ----------------------------- */

Value ExpressionType::evaluate(const Document& root) const {
    Value val(vpOperand[0]->evaluate(root));
    return Value(StringData(typeName(val.getType())));
}

REGISTER_EXPRESSION(type, ExpressionType::parse);
const char* ExpressionType::getOpName() const {
    return "$type";
}

/* -------------------------- ExpressionZip ------------------------------ */

REGISTER_EXPRESSION(zip, ExpressionZip::parse);
intrusive_ptr<Expression> ExpressionZip::parse(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    BSONElement expr,
    const VariablesParseState& vps) {
    uassert(34460,
            str::stream() << "$zip only supports an object as an argument, found "
                          << typeName(expr.type()),
            expr.type() == Object);

    intrusive_ptr<ExpressionZip> newZip(new ExpressionZip(expCtx));

    for (auto&& elem : expr.Obj()) {
        const auto field = elem.fieldNameStringData();
        if (field == "inputs") {
            uassert(34461,
                    str::stream() << "inputs must be an array of expressions, found "
                                  << typeName(elem.type()),
                    elem.type() == Array);
            for (auto&& subExpr : elem.Array()) {
                newZip->_inputs.push_back(parseOperand(expCtx, subExpr, vps));
            }
        } else if (field == "defaults") {
            uassert(34462,
                    str::stream() << "defaults must be an array of expressions, found "
                                  << typeName(elem.type()),
                    elem.type() == Array);
            for (auto&& subExpr : elem.Array()) {
                newZip->_defaults.push_back(parseOperand(expCtx, subExpr, vps));
            }
        } else if (field == "useLongestLength") {
            uassert(34463,
                    str::stream() << "useLongestLength must be a bool, found "
                                  << typeName(expr.type()),
                    elem.type() == Bool);
            newZip->_useLongestLength = elem.Bool();
        } else {
            uasserted(34464,
                      str::stream() << "$zip found an unknown argument: " << elem.fieldName());
        }
    }

    void ExpressionWeek::addOperand(const intrusive_ptr<Expression> &pExpression) {
        checkArgLimit(1);
        ExpressionNary::addOperand(pExpression);
    }

    Value ExpressionWeek::evaluate(const Document& pDocument) const {
        checkArgCount(1);
        Value pDate(vpOperand[0]->evaluate(pDocument));
        tm date = pDate.coerceToTm();
        int dayOfWeek = date.tm_wday;
        int dayOfYear = date.tm_yday;
        int prevSundayDayOfYear = dayOfYear - dayOfWeek; // may be negative
        int nextSundayDayOfYear = prevSundayDayOfYear + 7; // must be positive

Value ExpressionZip::evaluate(const Document& root) const {
    // Evaluate input values.
    vector<vector<Value>> inputValues;
    inputValues.reserve(_inputs.size());

    size_t minArraySize = 0;
    size_t maxArraySize = 0;
    for (size_t i = 0; i < _inputs.size(); i++) {
        Value evalExpr = _inputs[i]->evaluate(root);
        if (evalExpr.nullish()) {
            return Value(BSONNULL);
        }

        return Value::createInt(nextSundayWeek);
    }

    vector<Value> evaluatedDefaults(_inputs.size(), Value(BSONNULL));

    // If we need default values, evaluate each expression.
    if (minArraySize != maxArraySize) {
        for (size_t i = 0; i < _defaults.size(); i++) {
            evaluatedDefaults[i] = _defaults[i]->evaluate(root);
        }
    }

    /* ------------------------- ExpressionYear ----------------------------- */

    ExpressionYear::~ExpressionYear() {
    }

    intrusive_ptr<ExpressionNary> ExpressionYear::create() {
        intrusive_ptr<ExpressionYear> pExpression(new ExpressionYear());
        return pExpression;
    }

    ExpressionYear::ExpressionYear():
        ExpressionNary() {
    }

    void ExpressionYear::addOperand(
        const intrusive_ptr<Expression> &pExpression) {
        checkArgLimit(1);
        ExpressionNary::addOperand(pExpression);
    }

    Value ExpressionYear::evaluate(const Document& pDocument) const {
        checkArgCount(1);
        Value pDate(vpOperand[0]->evaluate(pDocument));
        tm date = pDate.coerceToTm();
        return Value::createInt(date.tm_year + 1900); // tm_year is years since 1900
    }

    const char *ExpressionYear::getOpName() const {
        return "$year";
    }

}
